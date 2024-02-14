package jfrparser

import (
	"regexp"

	"github.com/grafana/jfr-parser/parser/types"
)

// reference: https://github.com/grafana/jfr-parser/blob/main/pprof/symbols.go

var (
	// jdk/internal/reflect/GeneratedMethodAccessor31
	generatedMethodAccessor = regexp.MustCompile(`^(jdk/internal/reflect/GeneratedMethodAccessor)(\d+)$`)

	// org/example/rideshare/OrderService$$Lambda$669.0x0000000800fd7318.run
	// Fib$$Lambda.0x00007ffa600c4da0.run
	lambda = regexp.MustCompile(`^(.+\$\$Lambda)(\$?\d*[./](0x)?[\da-f]+|\d+)$`)

	// libzstd-jni-1.5.1-16931311898282279136.so.Java_com_github_luben_zstd_ZstdInputStreamNoFinalizer_decompressStream
	libzstd = regexp.MustCompile(`^(\.?/tmp/)?(libzstd-jni-\d+\.\d+\.\d+-)(\d+)(\.so)( \(deleted\))?$`)

	// ./tmp/libamazonCorrettoCryptoProvider109b39cf33c563eb.so
	// ./tmp/amazonCorrettoCryptoProviderNativeLibraries.7382c2f79097f415/libcrypto.so (deleted)
	libcrypto = regexp.MustCompile(`^(\.?/tmp/)?(lib)?(amazonCorrettoCryptoProvider)(NativeLibraries\.)?([0-9a-f]{16})(/libcrypto|/libamazonCorrettoCryptoProvider)?(\.so)( \(deleted\))?$`)

	// libasyncProfiler-linux-arm64-17b9a1d8156277a98ccc871afa9a8f69215f92.so
	libasyncProfiler = regexp.MustCompile(`^(\.?/tmp/)?(libasyncProfiler)-(linux-arm64|linux-musl-x64|linux-x64|macos)-(17b9a1d8156277a98ccc871afa9a8f69215f92)(\.so)( \(deleted\))?$`)

	// TODO: ./tmp/snappy-1.1.8-6fb9393a-3093-4706-a7e4-837efe01d078-libsnappyjava.so
)

func cleanse(frame string) string {
	frame = generatedMethodAccessor.ReplaceAllString(frame, "${1}_")
	frame = lambda.ReplaceAllString(frame, "${1}_")
	frame = libzstd.ReplaceAllString(frame, "libzstd-jni-_.so")
	frame = libcrypto.ReplaceAllString(frame, "libamazonCorrettoCryptoProvider_.so")
	frame = libasyncProfiler.ReplaceAllString(frame, "libasyncProfiler-_.so")
	return frame
}

func processSyms(ref *types.SymbolList) {
	for i := range ref.Symbol {
		ref.Symbol[i].String = cleanse(ref.Symbol[i].String)
	}
}
