/**
 * A simple k6 load test for pyroscope pipeline.
 * 
 * Usage:
 * - Average load test:
 * k6 run pyroscope_pipeline_k6_load_test.js
 * - Breakpoint test:
 * k6 run pyroscope_pipeline_k6_load_test.js -e WORKLOAD=breakpoint
 * - Smoke test:
 * k6 run pyroscope_pipeline_k6_load_test.js -e WORKLOAD=smoke
 */
import http from 'k6/http';
import { check } from 'k6';

var averageWorkload = {
  // example workload: 30k pods with 5m upload interval, assuming uniform random distribution, 
  // then average rps is 30,000/5/60=100
  load_avg: {
    executor: "ramping-vus",
    stages: [
      { duration: "10s", target: 100 },
      { duration: "50s", target: 100 },
      { duration: "5s", target: 0 },
    ],
  },
};

var breakpointWorkload = {
  load_avg: {
    executor: "ramping-vus",
    stages: [
      { duration: "10s", target: 20 },
      { duration: "30s", target: 20 },
      { duration: "30s", target: 100 },
      { duration: "30s", target: 120 },
      { duration: "30s", target: 150 },
      { duration: "30s", target: 180 },
      { duration: "30s", target: 200 },
    ],
  },
};

export const options = 'smoke' === __ENV.WORKLOAD ? null : {
  thresholds: {
    http_req_failed: [{threshold: "rate<0.000000001", abortOnFail: true }],
    http_req_duration: ["med<100"],
    http_req_duration: ["p(95)<300"],
    http_req_duration: ["p(99)<1000"],
    http_req_duration: ["max<3000"],
  },
  scenarios: 'breakpoint' === __ENV.WORKLOAD ? breakpointWorkload : averageWorkload,
};

const dist = [
  {
    urlParams: {
      "name":       "com.example.Test{dc=us-east-1,kubernetes_pod_name=app-abcd1234}",
      "from":       "1700332322",
      "until":      "1700332329",
      "format":     "jfr",
      "sampleRate": "100",
    },
    jfrgz: open("../receiver/pyroscopereceiver/testdata/cortex-dev-01__kafka-0__cpu__0.jfr.gz"/*unix-only*/, "b"),
  },
  {
    urlParams: {
      "name":   "com.example.Test{dc=us-east-1,kubernetes_pod_name=app-abcd1234}",
      "from":   "1700332322",
      "until":  "1700332329",
      "format": "jfr",
    },
    jfrgz: open("../receiver/pyroscopereceiver/testdata/memory_alloc_live_example.jfr.gz"/*unix-only*/, "b"),
  },
]
const collectorAddr = "http://0.0.0.0:8062"

let j = 0

export default function () {
  const data = {
    jfr: http.file(dist[j].jfrgz, "jfr"),
  };
  const params = dist[j].urlParams
  const qs = Object.keys(params).map(k => `${k}=${params[k]}`).join("&")
  const res = http.post(`${collectorAddr}/ingest?${encodeURI(qs)}`, data);
  
  check(res, {
    "response code was 204": (res) => res.status == 204,
  });
  j = (j + 1) % dist.length;
}
