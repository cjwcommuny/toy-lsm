window.BENCHMARK_DATA = {
  "lastUpdate": 1726726649953,
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
      },
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
          "id": "8543300cdbb17c6b49365427e479e153d9f1e960",
          "message": "fix: SST deletion (#95)",
          "timestamp": "2024-09-18T23:04:23+08:00",
          "tree_id": "f0436ce3c96d19a3a0a75b2abc3737bacc8e5f25",
          "url": "https://github.com/cjwcommuny/toy-lsm/commit/8543300cdbb17c6b49365427e479e153d9f1e960"
        },
        "date": 1726672753279,
        "tool": "cargo",
        "benches": [
          {
            "name": "rocks sequentially populate small value",
            "value": 30084467,
            "range": "± 226875301",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate small value",
            "value": 27407983,
            "range": "± 1851187",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread small value",
            "value": 7210334,
            "range": "± 634586",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate small value",
            "value": 1781340,
            "range": "± 9348",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate large value",
            "value": 303730445,
            "range": "± 2805871",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate large value",
            "value": 294475440,
            "range": "± 4081753",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread large value",
            "value": 29365850,
            "range": "± 3122705",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate large value",
            "value": 32349824,
            "range": "± 3570215",
            "unit": "ns/iter"
          },
          {
            "name": "mydb sequentially populate small value",
            "value": 124739591,
            "range": "± 4570304",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randomly populate small value",
            "value": 131616257,
            "range": "± 6313231",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randread small value",
            "value": 42611925,
            "range": "± 636247",
            "unit": "ns/iter"
          },
          {
            "name": "mydb iterate small value",
            "value": 13551923,
            "range": "± 155665",
            "unit": "ns/iter"
          },
          {
            "name": "mydb sequentially populate large value",
            "value": 679393625,
            "range": "± 6799178",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randomly populate large value",
            "value": 668623720,
            "range": "± 4895243",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randread large value",
            "value": 231366658,
            "range": "± 6404802",
            "unit": "ns/iter"
          },
          {
            "name": "mydb iterate large value",
            "value": 16180207,
            "range": "± 212792",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "6c018b4e075b33cc3e892119629878b4e4527a90",
          "message": "ci: add cache (#96)",
          "timestamp": "2024-09-19T14:02:57+08:00",
          "tree_id": "22f4326265c2f4e8faa207deeae9974594843384",
          "url": "https://github.com/cjwcommuny/toy-lsm/commit/6c018b4e075b33cc3e892119629878b4e4527a90"
        },
        "date": 1726726649141,
        "tool": "cargo",
        "benches": [
          {
            "name": "rocks sequentially populate small value",
            "value": 24723721,
            "range": "± 7507633",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate small value",
            "value": 25498497,
            "range": "± 3739339",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread small value",
            "value": 7324890,
            "range": "± 786807",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate small value",
            "value": 1768027,
            "range": "± 15101",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate large value",
            "value": 280866189,
            "range": "± 1977037",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate large value",
            "value": 275129455,
            "range": "± 4003360",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread large value",
            "value": 27894365,
            "range": "± 2299534",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate large value",
            "value": 32088296,
            "range": "± 2823494",
            "unit": "ns/iter"
          },
          {
            "name": "mydb sequentially populate small value",
            "value": 88237144,
            "range": "± 2152687",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randomly populate small value",
            "value": 94272093,
            "range": "± 11951054",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randread small value",
            "value": 39757854,
            "range": "± 163140",
            "unit": "ns/iter"
          },
          {
            "name": "mydb iterate small value",
            "value": 12769306,
            "range": "± 76237",
            "unit": "ns/iter"
          },
          {
            "name": "mydb sequentially populate large value",
            "value": 640898645,
            "range": "± 3984545",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randomly populate large value",
            "value": 638003257,
            "range": "± 4939654",
            "unit": "ns/iter"
          },
          {
            "name": "mydb randread large value",
            "value": 221829461,
            "range": "± 3657948",
            "unit": "ns/iter"
          },
          {
            "name": "mydb iterate large value",
            "value": 15453133,
            "range": "± 166172",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}