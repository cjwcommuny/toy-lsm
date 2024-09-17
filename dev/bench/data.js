window.BENCHMARK_DATA = {
  "lastUpdate": 1726577930037,
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
          "id": "28bc02bea655c08daa7f209378b8d37b1e75d8e2",
          "message": "test: add integration test (#93)",
          "timestamp": "2024-09-17T17:39:57+08:00",
          "tree_id": "d08256612e72ce38f4cbf676779ff30fc1371041",
          "url": "https://github.com/cjwcommuny/toy-lsm/commit/28bc02bea655c08daa7f209378b8d37b1e75d8e2"
        },
        "date": 1726577929626,
        "tool": "cargo",
        "benches": [
          {
            "name": "rocks sequentially populate small value",
            "value": 26239963,
            "range": "± 6058884",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate small value",
            "value": 21591092,
            "range": "± 1956706",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread small value",
            "value": 6594326,
            "range": "± 96823",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate small value",
            "value": 1707539,
            "range": "± 15839",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate large value",
            "value": 261696189,
            "range": "± 2936751",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate large value",
            "value": 253712812,
            "range": "± 3652347",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread large value",
            "value": 26894657,
            "range": "± 2730444",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate large value",
            "value": 30665614,
            "range": "± 2268546",
            "unit": "ns/iter"
          },
          {
            "name": "mydb sequentially populate small value",
            "value": 69940728,
            "range": "± 3703046",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randomly populate small value",
            "value": 75118190,
            "range": "± 6365700",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randread small value",
            "value": 37852716,
            "range": "± 424488",
            "unit": "ns/iter"
          },
          {
            "name": "mydb iterate small value",
            "value": 12303401,
            "range": "± 127137",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate large value #2",
            "value": 574153676,
            "range": "± 5286699",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randomly populate large value",
            "value": 573312378,
            "range": "± 6540090",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randread large value",
            "value": 203673207,
            "range": "± 2924684",
            "unit": "ns/iter"
          },
          {
            "name": "mydb iterate large value",
            "value": 14812270,
            "range": "± 197331",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}