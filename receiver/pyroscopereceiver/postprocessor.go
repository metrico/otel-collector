package pyroscopereceiver

import (
	"encoding/binary"
	"fmt"
	"github.com/go-faster/city"
	_ "github.com/go-faster/city"
	"github.com/google/pprof/profile"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type profTrieNode struct {
	parentId uint64
	funcId   uint64
	nodeId   uint64
	values   []profTrieValue
}

type profTrieValue struct {
	name  string
	self  int64
	total int64
}

const kMul = 0x9ddfea08eb382d69

func hash128To64(l uint64, h uint64) uint64 {
	// Murmur-inspired hashing.
	var a = (l ^ h) * kMul
	a ^= (a >> 47)
	var b = (h ^ a) * kMul
	b ^= (b >> 47)
	b *= kMul
	return b
}

func getNodeId(parentId uint64, funcId uint64, traceLevel int) uint64 {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], parentId)
	binary.LittleEndian.PutUint64(buf[8:16], funcId)
	if traceLevel > 511 {
		traceLevel = 511
	}
	return city.CH64(buf)>>9 | (uint64(traceLevel) << 55)
}

func postProcessProf(profile *profile.Profile, attrs *pcommon.Map) {
	funcs := map[uint64]string{}
	tree := map[uint64]*profTrieNode{}
	_values := make([]profTrieValue, len(profile.SampleType))
	for i, name := range profile.SampleType {
		_values[i] = profTrieValue{
			name: fmt.Sprintf("%s:%s", name.Type, name.Unit),
		}
	}
	for _, sample := range profile.Sample {
		parentId := uint64(0)
		for i := len(sample.Location) - 1; i >= 0; i-- {
			loc := sample.Location[i]
			name := "n/a"
			if len(loc.Line) > 0 {
				name = loc.Line[0].Function.Name
			}
			fnId := city.CH64([]byte(name))
			funcs[fnId] = name
			nodeId := getNodeId(parentId, fnId, len(sample.Location)-i)
			node := tree[nodeId]
			if node == nil {
				values := make([]profTrieValue, len(profile.SampleType))
				copy(values, _values)
				node = &profTrieNode{
					parentId: parentId,
					funcId:   fnId,
					nodeId:   nodeId,
					values:   values,
				}
				tree[nodeId] = node
			}
			for j := range node.values {
				node.values[j].total += sample.Value[j]
				if i == 0 {
					node.values[j].self += sample.Value[j]
				}
			}
			parentId = nodeId
		}
	}
	var bFnMap []byte
	bFnMap = binary.AppendVarint(bFnMap, int64(len(funcs)))
	for fnId, name := range funcs {
		bFnMap = binary.AppendUvarint(bFnMap, fnId)
		bFnMap = binary.AppendVarint(bFnMap, int64(len(name)))
		bFnMap = append(bFnMap, name...)
	}
	fnMap := attrs.PutEmptyBytes("functions")
	fnMap.FromRaw(bFnMap)

	var bNodeMap []byte
	bNodeMap = binary.AppendVarint(bNodeMap, int64(len(tree)))
	for _, node := range tree {
		bNodeMap = binary.AppendUvarint(bNodeMap, node.parentId)
		bNodeMap = binary.AppendUvarint(bNodeMap, node.funcId)
		bNodeMap = binary.AppendUvarint(bNodeMap, node.nodeId)
		bNodeMap = binary.AppendVarint(bNodeMap, int64(len(node.values)))
		for _, v := range node.values {
			bNodeMap = binary.AppendVarint(bNodeMap, int64(len(v.name)))
			bNodeMap = append(bNodeMap, v.name...)
			bNodeMap = binary.AppendVarint(bNodeMap, v.self)
			bNodeMap = binary.AppendVarint(bNodeMap, v.total)
		}
	}
	treeMap := attrs.PutEmptyBytes("tree")
	treeMap.FromRaw(bNodeMap)
}
