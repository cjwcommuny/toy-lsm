window.BENCHMARK_DATA = {
  "lastUpdate": 1726580215345,
  "repoUrl": "https://github.com/cjwcommuny/toy-lsm",
  "entries": {
    "Benchmark with RocksDB": [
      {
        "commit": {
          "author": {
            "email": "cjwcommuny@outlook.com",
            "name": "cjw",
            "username": "cjwcommuny"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c4966d7c819afda90c638a7c32b2a398c21d3643",
          "message": "fix: bench (#94)",
          "timestamp": "2024-09-17T21:30:40+08:00",
          "tree_id": "e853f655c3cd94e691ea77b49067c799e838d9f4",
          "url": "https://github.com/cjwcommuny/toy-lsm/commit/c4966d7c819afda90c638a7c32b2a398c21d3643"
        },
        "date": 1726580214917,
        "tool": "cargo",
        "benches": [
          {
            "name": "rocks sequentially populate small value",
            "value": 29755879,
            "range": "± 6000541",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate small value",
            "value": 23722098,
            "range": "± 1329372",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread small value",
            "value": 6963308,
            "range": "± 75649",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate small value",
            "value": 1785202,
            "range": "± 11287",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate large value",
            "value": 316103052,
            "range": "± 7392494",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate large value",
            "value": 305847220,
            "range": "± 9480015",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread large value",
            "value": 29590429,
            "range": "± 2703815",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate large value",
            "value": 33578020,
            "range": "± 1533465",
            "unit": "ns/iter"
          },
          {
            "name": "mydb sequentially populate small value",
            "value": 89430402,
            "range": "± 6095157",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randomly populate small value",
            "value": 95763553,
            "range": "± 5594464",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randread small value",
            "value": 40115930,
            "range": "± 427625",
            "unit": "ns/iter"
          },
          {
            "name": "mydb iterate small value",
            "value": 12841258,
            "range": "± 70384",
            "unit": "ns/iter"
          },
          {
            "name": "mydb sequentially populate large value",
            "value": 677393611,
            "range": "± 11422179",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randomly populate large value",
            "value": 677162715,
            "range": "± 11618667",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randread large value",
            "value": 216825367,
            "range": "± 3209505",
            "unit": "ns/iter"
          },
          {
            "name": "mydb iterate large value",
            "value": 15491214,
            "range": "± 156509",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}