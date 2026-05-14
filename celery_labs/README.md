First need to have a python file with Celery initialized and python function enclosed under `@app.task` decorator
Docker Shell - `docker exec -it celery_worker bash`
Run Celery Worker - `celery -A celery_app worker --loglevel=info -E`
Test Run the task created in celery_app.py - `docker exec celery_worker python -c "from celery_app import add; print(add.delay(10, 20))"`

Task calling methods
- Delay - `add.delay(2, 2)`
- Async (Queue priority, Countdown after how much time request is made) - `add.apply_async((2, 2), queue='lopri', countdown=10)`
- To add persistency to the tasks recording we add backend to redis-port

Shared tasks - `from celery import shared_task` and functions are enclosed with `@shared_task`

### The broker_url variable name is fixed
`
broker_url = "redis://redis:6379/0"
`
### The result_backend variable name is fixed
`
result_backend = "redis://redis:6379/1"
`
### Add Task priority
`
task_routes = {
    'tasks.math.add': {'priority': 9},
    'tasks.math.mul': {'priority': 0},
}
`
### Task Annotations (like rate limiting)
`
task_annotations = {
    'tasks.math.square': {'rate_limit': '2/m'}
}
`
### Adding workers has two different meaning
- 1. Adding concurrency
- 2. Different worker for different work instances

### To run Schedule (Celery Beat)
- `celery -A celery_app beat --loglevel=info`

### What does celerybeat-schedule stores
- last execution timestamp
- next scheduled run
- task metadata
- schedule hash

### To debug schedule check CLI command for redis
`LRANGE celery 0 -1`

## Concurrency models (how Celery executes tasks)
### A. prefork (default)
- Uses multiple OS processes
- Each process runs independently
`
Master process
   ↓ forks
Worker 1 (process)
Worker 2 (process)
Worker 3 (process)
`
`
Worker Master
 ├── Process 1 → task
 ├── Process 2 → task
 ├── Process 3 → task
 └── Process 4 → task
 `
#### CLI Commands for prefork
`
celery -A celery_app worker --pool=prefork --concurrency=4
`
### B. threads
- Uses Python threads instead of processes
#### CLI Commands for threads
`
celery -A celery_app worker --pool=threads --concurrency=10
`
### C. gevent
- Greenlets (cooperative lightweight threads)
`
celery -A celery_app worker --pool=gevent --concurrency=100
`
### D. eventlet
- Similar to gevent but older ecosystem.
`
celery -A celery_app worker --pool=eventlet --concurrency=100
`

## Playing with worker configurations
```
celery -A celery_app worker \  # starting the worker
  --pool=prefork \             # selecting the concurrency model
  --concurrency=4 \            # Setting value of concurrency
  --autoscale=10,3 \           # Autoscaling worker value (10 is max, 3 is min)
  --prefetch-multiplier=1 \    # reserve task and pass on to workers before execution
  --max-tasks-per-child=100 \  # Number of tasks per worker
  --max-memory-per-child=20000 # Memory limit per worker (here we set it to 20MB)
```
### Here we use message abstraction layer Kombu that sits between Celery and Redis/RabbitMQ
- `from kombu import Queue`
- CodeSnippet for custom queue
```
task_queues = {
    Queue('delayed_task_queue'),
}
```
- CLI command to run custom queue - `celery -A celery_app worker -n worker1@%h -Q delayed_task_queue --loglevel=info -E -n delayed-custom-queue --pool=prefork --concurrency=2 --prefetch-multiplier=2 --max-tasks-per-child=100 --max-memory-per-child=30000` ;
`celery -A celery_app worker -n worker2@%h -Q etl_queue --loglevel=info -E -n etl-custom-queue`
`celery -A celery_app worker -n worker3@%h -Q retry_task_queue --loglevel=info -E -n retry_task_queue`

### Retires
```
except Exception as e:
    raise self.retry(exc=e, countdown=60, max_retries=3)
```
```
@shared_task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5, 'countdown': 10}, retry_backoff=True)
```

### Logging
```
import logging
logger = logging.getLogger(__name__)
logger.info("This is an info log from the log_task.")
logger.warning("This is a warning log from the log_task.")
logger.error("This is an error log from the log_task.")
```

### Shell Scripts
```
celery -A celery_app worker -n worker6@%h --loglevel=info -E -n idle_worker
celery -A celery_app worker -n log_task_worker4@%h -Q log_task_queue --loglevel=info -E
celery -A celery_app worker -n retry_task_worker2@%h -Q retry_task_queue --loglevel=info -E
celery -A celery_app worker -n etl_queue_worker1@%h -Q etl_queue --loglevel=info -E
celery -A celery_app worker -n worker3@%h -Q delayed_task_queue --loglevel=info -E -n delayed-custom-queue --pool=prefork --concurrency=2 --prefetch-multiplier=2 --max-tasks-per-child=100 --max-memory-per-child=300000
celery -A celery_app worker -n math_queue_worker5@%h -Q math_task_queue --loglevel=info -E

celery -A celery_app worker --pool=prefork --concurrency=4 --autoscale=10,3 --prefetch-multiplier=1 --max-tasks-per-child=100 --max-memory-per-child=200000

docker exec -it celery_worker python
docker exec -it redis redis-cli
docker exec -it celery_worker bash
celery -A celery_app beat --loglevel=info
cd celery_labs && source venv/bin/activate
```

### Python Shell Testing Code
```
from celery import chain
from tasks.math import add, mul, square
from tasks.delay import delay_task
from tasks.etl import ingestion_task, transformation_task, loading_task
from tasks.logger import log_task
from tasks.retries import retry_task
import time

# Loop to call add task asynchronously 100 times with 10-second delay
for i in range(100):
    result = add.delay(i, i+1)
    print(f"add task id: {result.id}")
    time.sleep(10)

# Loop to call mul task asynchronously 100 times with 10-second delay
for i in range(100):
    result = mul.delay(i, i+2)
    print(f"mul task id: {result.id}")
    time.sleep(10)

# Calling square task asynchronously once
print(square.delay(9))

# Loop to call square task asynchronously multiple times
for i in range(10):
    print(square.delay(i * i))

# Loop to call delay_task asynchronously multiple times with 800ms delay
for i in range(20):
    print(delay_task.delay(800))

# Loop to call delay_task asynchronously multiple times with 1000ms delay
for i in range(100):
    print(delay_task.delay(1000))

# Run ETL pipeline as a chain of tasks, with a 10-second pause between each chain execution
for i in range(10):
    result = chain(ingestion_task.s(), transformation_task.s(), loading_task.s()).apply_async()
    print(result)
    time.sleep(10)

# Run log_task in a loop 100 times with increasing delay (5*i seconds)
for i in range(100):
    result = log_task.delay()
    print(result.id)
    time.sleep(5 * i)

# Run retry_task 150 times to increase occurrence of errors
for _ in range(150):
    res = retry_task.delay()
    print("task_id:", res.id)

    # Poll task state every 2 seconds until SUCCESS or FAILURE
    while True:
        res_state = res.state
        print("state:", res_state)
        if res_state in ("SUCCESS", "FAILURE"):
            break
        time.sleep(2)

    print("final:", res.state)
    if res.successful():
        print("result:", res.result)
    else:
        print("error:", res.result)
```

### Queries Executed on Prometheus
- `{__name__=~"rabbitmq_queue_messages.*"}`
- `{__name__=~"celery_.*"}`
- `{__name__=~"redis_memory_.*"}`
- `{__name__=~"celery_.*|rabbitmq_queue_.*|redis_memory_.*"}`

### Queries Executed on Grafana
```
{
  "apiVersion": "dashboard.grafana.app/v2",
  "kind": "Dashboard",
  "metadata": {
    "name": "adwtk4f",
    "namespace": "default",
    "uid": "b536cad5-aeeb-4b51-b89f-dedebb79a269",
    "resourceVersion": "1778765661711998",
    "generation": 2,
    "creationTimestamp": "2026-05-14T13:29:32Z",
    "labels": {
      "grafana.app/deprecatedInternalID": "1275597560139776"
    },
    "annotations": {
      "grafana.app/createdBy": "user:afm116nphxlhcb",
      "grafana.app/folder": "",
      "grafana.app/saved-from-ui": "Grafana v13.0.1+security-01 (9bbe672d)",
      "grafana.app/updatedBy": "user:afm116nphxlhcb",
      "grafana.app/updatedTimestamp": "2026-05-14T13:34:21Z"
    }
  },
  "spec": {
    "annotations": [
      {
        "kind": "AnnotationQuery",
        "spec": {
          "query": {
            "kind": "DataQuery",
            "group": "grafana",
            "version": "v0",
            "datasource": {
              "name": "-- Grafana --"
            },
            "spec": {}
          },
          "enable": true,
          "hide": true,
          "iconColor": "rgba(0, 211, 255, 1)",
          "name": "Annotations & Alerts",
          "builtIn": true
        }
      }
    ],
    "cursorSync": "Off",
    "description": "A metrics dashboard pointing metrics of celery-rabbitmq-redis",
    "editable": true,
    "elements": {
      "panel-1": {
        "kind": "Panel",
        "spec": {
          "id": 1,
          "title": "Celery",
          "description": "Metrices of Celery",
          "links": [],
          "data": {
            "kind": "QueryGroup",
            "spec": {
              "queries": [
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "celery_tasks",
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "A",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "celery_workers",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "B",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rate(celery_task_latency_sum[1m])",
                        "format": "heatmap",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "C",
                    "hidden": false
                  }
                }
              ],
              "transformations": [],
              "queryOptions": {}
            }
          },
          "vizConfig": {
            "kind": "VizConfig",
            "group": "timeseries",
            "version": "13.0.1+security-01",
            "spec": {
              "options": {
                "annotations": {
                  "clustering": -1,
                  "multiLane": false
                },
                "legend": {
                  "calcs": [],
                  "displayMode": "list",
                  "placement": "bottom",
                  "showLegend": true
                },
                "tooltip": {
                  "hideZeros": false,
                  "mode": "single",
                  "sort": "none"
                }
              },
              "fieldConfig": {
                "defaults": {
                  "thresholds": {
                    "mode": "absolute",
                    "steps": [
                      {
                        "value": 0,
                        "color": "green"
                      },
                      {
                        "value": 80,
                        "color": "red"
                      }
                    ]
                  },
                  "color": {
                    "mode": "palette-classic"
                  },
                  "custom": {
                    "axisBorderShow": false,
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "barWidthFactor": 0.6,
                    "drawStyle": "line",
                    "fillOpacity": 25,
                    "gradientMode": "opacity",
                    "hideFrom": {
                      "legend": false,
                      "tooltip": false,
                      "viz": false
                    },
                    "insertNulls": false,
                    "lineInterpolation": "linear",
                    "lineStyle": {
                      "fill": "solid"
                    },
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": {
                      "type": "linear"
                    },
                    "showPoints": "auto",
                    "showValues": false,
                    "spanNulls": false,
                    "stacking": {
                      "group": "A",
                      "mode": "normal"
                    },
                    "thresholdsStyle": {
                      "mode": "off"
                    }
                  }
                },
                "overrides": []
              }
            }
          },
          "transparent": true
        }
      },
      "panel-10": {
        "kind": "Panel",
        "spec": {
          "id": 10,
          "title": "Redis Cache Hit Ratio",
          "description": "",
          "links": [],
          "data": {
            "kind": "QueryGroup",
            "spec": {
              "queries": [
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "code",
                        "expr": "rate(redis_keyspace_hits_total[5m])\r\n/\r\n(\r\n  rate(redis_keyspace_hits_total[5m])\r\n  +\r\n  rate(redis_keyspace_misses_total[5m])\r\n)",
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "A",
                    "hidden": false
                  }
                }
              ],
              "transformations": [],
              "queryOptions": {}
            }
          },
          "vizConfig": {
            "kind": "VizConfig",
            "group": "stat",
            "version": "13.0.1+security-01",
            "spec": {
              "options": {
                "colorMode": "value",
                "graphMode": "area",
                "justifyMode": "auto",
                "orientation": "auto",
                "percentChangeColorMode": "standard",
                "reduceOptions": {
                  "calcs": [
                    "lastNotNull"
                  ],
                  "fields": "",
                  "values": false
                },
                "showPercentChange": false,
                "textMode": "auto",
                "wideLayout": true
              },
              "fieldConfig": {
                "defaults": {
                  "thresholds": {
                    "mode": "absolute",
                    "steps": [
                      {
                        "value": 0,
                        "color": "green"
                      },
                      {
                        "value": 80,
                        "color": "red"
                      }
                    ]
                  },
                  "color": {
                    "mode": "shades",
                    "fixedColor": "#3d8bff"
                  }
                },
                "overrides": []
              }
            }
          }
        }
      },
      "panel-2": {
        "kind": "Panel",
        "spec": {
          "id": 2,
          "title": "RabbitMQ",
          "description": "Metrices of rabbitmq",
          "links": [],
          "data": {
            "kind": "QueryGroup",
            "spec": {
              "queries": [
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rabbitmq_queue_messages",
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "A",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rabbitmq_queue_messages_ready",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "B",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rabbitmq_queue_messages_unacked",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "C",
                    "hidden": false
                  }
                }
              ],
              "transformations": [],
              "queryOptions": {}
            }
          },
          "vizConfig": {
            "kind": "VizConfig",
            "group": "timeseries",
            "version": "13.0.1+security-01",
            "spec": {
              "options": {
                "annotations": {
                  "clustering": -1,
                  "multiLane": false
                },
                "legend": {
                  "calcs": [],
                  "displayMode": "list",
                  "placement": "bottom",
                  "showLegend": true
                },
                "tooltip": {
                  "hideZeros": false,
                  "mode": "single",
                  "sort": "none"
                }
              },
              "fieldConfig": {
                "defaults": {
                  "thresholds": {
                    "mode": "absolute",
                    "steps": [
                      {
                        "value": 0,
                        "color": "green"
                      },
                      {
                        "value": 80,
                        "color": "red"
                      }
                    ]
                  },
                  "color": {
                    "mode": "palette-classic"
                  },
                  "custom": {
                    "axisBorderShow": false,
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "barWidthFactor": 0.6,
                    "drawStyle": "line",
                    "fillOpacity": 15,
                    "gradientMode": "opacity",
                    "hideFrom": {
                      "legend": false,
                      "tooltip": false,
                      "viz": false
                    },
                    "insertNulls": false,
                    "lineInterpolation": "linear",
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": {
                      "type": "linear"
                    },
                    "showPoints": "auto",
                    "showValues": false,
                    "spanNulls": false,
                    "stacking": {
                      "group": "A",
                      "mode": "none"
                    },
                    "thresholdsStyle": {
                      "mode": "off"
                    }
                  }
                },
                "overrides": []
              }
            }
          }
        }
      },
      "panel-3": {
        "kind": "Panel",
        "spec": {
          "id": 3,
          "title": "RabbitMQ Consumers and Producers",
          "description": "",
          "links": [],
          "data": {
            "kind": "QueryGroup",
            "spec": {
              "queries": [
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rabbitmq_global_consumers",
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "A",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rate(rabbitmq_global_messages_received_total[5m])",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "B",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rate(rabbitmq_global_messages_delivered_total[5m])",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "C",
                    "hidden": false
                  }
                }
              ],
              "transformations": [],
              "queryOptions": {}
            }
          },
          "vizConfig": {
            "kind": "VizConfig",
            "group": "timeseries",
            "version": "13.0.1+security-01",
            "spec": {
              "options": {
                "annotations": {
                  "clustering": -1,
                  "multiLane": false
                },
                "legend": {
                  "calcs": [],
                  "displayMode": "list",
                  "placement": "bottom",
                  "showLegend": true
                },
                "tooltip": {
                  "hideZeros": false,
                  "mode": "single",
                  "sort": "none"
                }
              },
              "fieldConfig": {
                "defaults": {
                  "thresholds": {
                    "mode": "absolute",
                    "steps": [
                      {
                        "value": 0,
                        "color": "green"
                      },
                      {
                        "value": 80,
                        "color": "red"
                      }
                    ]
                  },
                  "color": {
                    "mode": "palette-classic"
                  },
                  "custom": {
                    "axisBorderShow": false,
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "barWidthFactor": 0.6,
                    "drawStyle": "line",
                    "fillOpacity": 15,
                    "gradientMode": "opacity",
                    "hideFrom": {
                      "legend": false,
                      "tooltip": false,
                      "viz": false
                    },
                    "insertNulls": false,
                    "lineInterpolation": "smooth",
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": {
                      "type": "linear"
                    },
                    "showPoints": "auto",
                    "showValues": false,
                    "spanNulls": false,
                    "stacking": {
                      "group": "A",
                      "mode": "none"
                    },
                    "thresholdsStyle": {
                      "mode": "off"
                    }
                  }
                },
                "overrides": []
              }
            }
          }
        }
      },
      "panel-4": {
        "kind": "Panel",
        "spec": {
          "id": 4,
          "title": "RabbitMQ Connection",
          "description": "",
          "links": [],
          "data": {
            "kind": "QueryGroup",
            "spec": {
              "queries": [
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rabbitmq_connections",
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "A",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rate(rabbitmq_connections_opened_total[1m])",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "B",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rate(rabbitmq_connections_closed_total[1m])",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "C",
                    "hidden": false
                  }
                }
              ],
              "transformations": [],
              "queryOptions": {}
            }
          },
          "vizConfig": {
            "kind": "VizConfig",
            "group": "timeseries",
            "version": "13.0.1+security-01",
            "spec": {
              "options": {
                "annotations": {
                  "clustering": -1,
                  "multiLane": false
                },
                "legend": {
                  "calcs": [],
                  "displayMode": "list",
                  "placement": "bottom",
                  "showLegend": true
                },
                "tooltip": {
                  "hideZeros": false,
                  "mode": "single",
                  "sort": "none"
                }
              },
              "fieldConfig": {
                "defaults": {
                  "thresholds": {
                    "mode": "absolute",
                    "steps": [
                      {
                        "value": 0,
                        "color": "green"
                      },
                      {
                        "value": 80,
                        "color": "red"
                      }
                    ]
                  },
                  "color": {
                    "mode": "palette-classic"
                  },
                  "custom": {
                    "axisBorderShow": false,
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "barWidthFactor": 0.6,
                    "drawStyle": "line",
                    "fillOpacity": 15,
                    "gradientMode": "opacity",
                    "hideFrom": {
                      "legend": false,
                      "tooltip": false,
                      "viz": false
                    },
                    "insertNulls": false,
                    "lineInterpolation": "smooth",
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": {
                      "type": "linear"
                    },
                    "showPoints": "auto",
                    "showValues": false,
                    "spanNulls": false,
                    "stacking": {
                      "group": "A",
                      "mode": "none"
                    },
                    "thresholdsStyle": {
                      "mode": "off"
                    }
                  }
                },
                "overrides": []
              }
            }
          }
        }
      },
      "panel-5": {
        "kind": "Panel",
        "spec": {
          "id": 5,
          "title": "RabbitMQ Memory Usage",
          "description": "",
          "links": [],
          "data": {
            "kind": "QueryGroup",
            "spec": {
              "queries": [
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rabbitmq_process_resident_memory_bytes",
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "A",
                    "hidden": false
                  }
                }
              ],
              "transformations": [],
              "queryOptions": {}
            }
          },
          "vizConfig": {
            "kind": "VizConfig",
            "group": "timeseries",
            "version": "13.0.1+security-01",
            "spec": {
              "options": {
                "annotations": {
                  "clustering": -1,
                  "multiLane": false
                },
                "legend": {
                  "calcs": [],
                  "displayMode": "list",
                  "placement": "bottom",
                  "showLegend": true
                },
                "tooltip": {
                  "hideZeros": false,
                  "mode": "single",
                  "sort": "none"
                }
              },
              "fieldConfig": {
                "defaults": {
                  "thresholds": {
                    "mode": "absolute",
                    "steps": [
                      {
                        "value": 0,
                        "color": "green"
                      },
                      {
                        "value": 80,
                        "color": "red"
                      }
                    ]
                  },
                  "color": {
                    "mode": "shades",
                    "fixedColor": "#ad0b20"
                  },
                  "custom": {
                    "axisBorderShow": false,
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "barWidthFactor": 0.6,
                    "drawStyle": "line",
                    "fillOpacity": 15,
                    "gradientMode": "opacity",
                    "hideFrom": {
                      "legend": false,
                      "tooltip": false,
                      "viz": false
                    },
                    "insertNulls": false,
                    "lineInterpolation": "smooth",
                    "lineStyle": {
                      "fill": "solid"
                    },
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": {
                      "type": "linear"
                    },
                    "showPoints": "auto",
                    "showValues": false,
                    "spanNulls": false,
                    "stacking": {
                      "group": "A",
                      "mode": "none"
                    },
                    "thresholdsStyle": {
                      "mode": "off"
                    }
                  }
                },
                "overrides": []
              }
            }
          },
          "transparent": true
        }
      },
      "panel-6": {
        "kind": "Panel",
        "spec": {
          "id": 6,
          "title": "RabbitMQ Connections",
          "description": "",
          "links": [],
          "data": {
            "kind": "QueryGroup",
            "spec": {
              "queries": [
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rabbitmq_process_max_tcp_sockets",
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "A",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rabbitmq_process_open_fds",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "B",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rabbitmq_process_open_tcp_sockets",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "C",
                    "hidden": false
                  }
                }
              ],
              "transformations": [],
              "queryOptions": {}
            }
          },
          "vizConfig": {
            "kind": "VizConfig",
            "group": "timeseries",
            "version": "13.0.1+security-01",
            "spec": {
              "options": {
                "annotations": {
                  "clustering": -1,
                  "multiLane": false
                },
                "legend": {
                  "calcs": [],
                  "displayMode": "list",
                  "placement": "bottom",
                  "showLegend": true
                },
                "tooltip": {
                  "hideZeros": false,
                  "mode": "single",
                  "sort": "none"
                }
              },
              "fieldConfig": {
                "defaults": {
                  "thresholds": {
                    "mode": "absolute",
                    "steps": [
                      {
                        "value": 0,
                        "color": "green"
                      },
                      {
                        "value": 80,
                        "color": "red"
                      }
                    ]
                  },
                  "color": {
                    "mode": "palette-classic"
                  },
                  "custom": {
                    "axisBorderShow": false,
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "barWidthFactor": 0.6,
                    "drawStyle": "line",
                    "fillOpacity": 15,
                    "gradientMode": "opacity",
                    "hideFrom": {
                      "legend": false,
                      "tooltip": false,
                      "viz": false
                    },
                    "insertNulls": false,
                    "lineInterpolation": "smooth",
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": {
                      "type": "linear"
                    },
                    "showPoints": "auto",
                    "showValues": false,
                    "spanNulls": false,
                    "stacking": {
                      "group": "A",
                      "mode": "none"
                    },
                    "thresholdsStyle": {
                      "mode": "off"
                    }
                  }
                },
                "overrides": []
              }
            }
          }
        }
      },
      "panel-7": {
        "kind": "Panel",
        "spec": {
          "id": 7,
          "title": "Redis Memory",
          "description": "",
          "links": [],
          "data": {
            "kind": "QueryGroup",
            "spec": {
              "queries": [
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "redis_memory_used_bytes",
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "A",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "redis_memory_used_peak_bytes",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "B",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "redis_memory_used_overhead_bytes",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "C",
                    "hidden": false
                  }
                }
              ],
              "transformations": [],
              "queryOptions": {}
            }
          },
          "vizConfig": {
            "kind": "VizConfig",
            "group": "timeseries",
            "version": "13.0.1+security-01",
            "spec": {
              "options": {
                "annotations": {
                  "clustering": -1,
                  "multiLane": false
                },
                "legend": {
                  "calcs": [],
                  "displayMode": "list",
                  "placement": "bottom",
                  "showLegend": true
                },
                "tooltip": {
                  "hideZeros": false,
                  "mode": "single",
                  "sort": "none"
                }
              },
              "fieldConfig": {
                "defaults": {
                  "thresholds": {
                    "mode": "absolute",
                    "steps": [
                      {
                        "value": 0,
                        "color": "green"
                      },
                      {
                        "value": 80,
                        "color": "red"
                      }
                    ]
                  },
                  "color": {
                    "mode": "palette-classic"
                  },
                  "custom": {
                    "axisBorderShow": false,
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "barWidthFactor": 0.6,
                    "drawStyle": "line",
                    "fillOpacity": 15,
                    "gradientMode": "opacity",
                    "hideFrom": {
                      "legend": false,
                      "tooltip": false,
                      "viz": false
                    },
                    "insertNulls": false,
                    "lineInterpolation": "smooth",
                    "lineStyle": {
                      "fill": "solid"
                    },
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": {
                      "type": "linear"
                    },
                    "showPoints": "auto",
                    "showValues": false,
                    "spanNulls": false,
                    "stacking": {
                      "group": "A",
                      "mode": "none"
                    },
                    "thresholdsStyle": {
                      "mode": "off"
                    }
                  }
                },
                "overrides": []
              }
            }
          }
        }
      },
      "panel-8": {
        "kind": "Panel",
        "spec": {
          "id": 8,
          "title": "Redis Connections",
          "description": "",
          "links": [],
          "data": {
            "kind": "QueryGroup",
            "spec": {
              "queries": [
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "redis_connected_clients",
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "A",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "redis_connected_slaves",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "B",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "redis_connections_received_total",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "C",
                    "hidden": false
                  }
                }
              ],
              "transformations": [],
              "queryOptions": {}
            }
          },
          "vizConfig": {
            "kind": "VizConfig",
            "group": "stat",
            "version": "13.0.1+security-01",
            "spec": {
              "options": {
                "colorMode": "value",
                "graphMode": "area",
                "justifyMode": "auto",
                "orientation": "auto",
                "percentChangeColorMode": "standard",
                "reduceOptions": {
                  "calcs": [
                    "lastNotNull"
                  ],
                  "fields": "",
                  "values": false
                },
                "showPercentChange": false,
                "textMode": "auto",
                "wideLayout": true
              },
              "fieldConfig": {
                "defaults": {
                  "thresholds": {
                    "mode": "absolute",
                    "steps": [
                      {
                        "value": 0,
                        "color": "green"
                      },
                      {
                        "value": 80,
                        "color": "red"
                      }
                    ]
                  },
                  "color": {
                    "mode": "continuous-GrYlRd"
                  }
                },
                "overrides": []
              }
            }
          }
        }
      },
      "panel-9": {
        "kind": "Panel",
        "spec": {
          "id": 9,
          "title": "Redis Commands",
          "description": "",
          "links": [],
          "data": {
            "kind": "QueryGroup",
            "spec": {
              "queries": [
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rate(redis_commands_processed_total[10m])",
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "A",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rate(redis_commands_rejected_calls_total[10m])",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "B",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rate(redis_commands_failed_calls_total[10m])",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "C",
                    "hidden": false
                  }
                },
                {
                  "kind": "PanelQuery",
                  "spec": {
                    "query": {
                      "kind": "DataQuery",
                      "group": "prometheus",
                      "version": "v0",
                      "datasource": {
                        "name": "cfm1cluho4bnka"
                      },
                      "spec": {
                        "editorMode": "builder",
                        "expr": "rate(redis_commands_latencies_usec_count[10m])",
                        "instant": false,
                        "legendFormat": "__auto",
                        "range": true
                      }
                    },
                    "refId": "D",
                    "hidden": false
                  }
                }
              ],
              "transformations": [],
              "queryOptions": {}
            }
          },
          "vizConfig": {
            "kind": "VizConfig",
            "group": "timeseries",
            "version": "13.0.1+security-01",
            "spec": {
              "options": {
                "annotations": {
                  "clustering": -1,
                  "multiLane": false
                },
                "legend": {
                  "calcs": [],
                  "displayMode": "list",
                  "placement": "bottom",
                  "showLegend": true
                },
                "tooltip": {
                  "hideZeros": false,
                  "mode": "single",
                  "sort": "none"
                }
              },
              "fieldConfig": {
                "defaults": {
                  "thresholds": {
                    "mode": "absolute",
                    "steps": [
                      {
                        "value": 0,
                        "color": "green"
                      },
                      {
                        "value": 80,
                        "color": "red"
                      }
                    ]
                  },
                  "color": {
                    "mode": "palette-classic"
                  },
                  "custom": {
                    "axisBorderShow": false,
                    "axisCenteredZero": false,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "barAlignment": 0,
                    "barWidthFactor": 0.6,
                    "drawStyle": "line",
                    "fillOpacity": 15,
                    "gradientMode": "opacity",
                    "hideFrom": {
                      "legend": false,
                      "tooltip": false,
                      "viz": false
                    },
                    "insertNulls": false,
                    "lineInterpolation": "linear",
                    "lineWidth": 1,
                    "pointSize": 5,
                    "scaleDistribution": {
                      "type": "linear"
                    },
                    "showPoints": "auto",
                    "showValues": false,
                    "spanNulls": false,
                    "stacking": {
                      "group": "A",
                      "mode": "none"
                    },
                    "thresholdsStyle": {
                      "mode": "off"
                    }
                  }
                },
                "overrides": []
              }
            }
          }
        }
      }
    },
    "layout": {
      "kind": "GridLayout",
      "spec": {
        "items": [
          {
            "kind": "GridLayoutItem",
            "spec": {
              "x": 0,
              "y": 0,
              "width": 12,
              "height": 8,
              "element": {
                "kind": "ElementReference",
                "name": "panel-1"
              }
            }
          },
          {
            "kind": "GridLayoutItem",
            "spec": {
              "x": 12,
              "y": 0,
              "width": 12,
              "height": 8,
              "element": {
                "kind": "ElementReference",
                "name": "panel-2"
              }
            }
          },
          {
            "kind": "GridLayoutItem",
            "spec": {
              "x": 0,
              "y": 8,
              "width": 12,
              "height": 8,
              "element": {
                "kind": "ElementReference",
                "name": "panel-3"
              }
            }
          },
          {
            "kind": "GridLayoutItem",
            "spec": {
              "x": 12,
              "y": 8,
              "width": 12,
              "height": 8,
              "element": {
                "kind": "ElementReference",
                "name": "panel-4"
              }
            }
          },
          {
            "kind": "GridLayoutItem",
            "spec": {
              "x": 0,
              "y": 16,
              "width": 12,
              "height": 8,
              "element": {
                "kind": "ElementReference",
                "name": "panel-5"
              }
            }
          },
          {
            "kind": "GridLayoutItem",
            "spec": {
              "x": 12,
              "y": 16,
              "width": 12,
              "height": 8,
              "element": {
                "kind": "ElementReference",
                "name": "panel-6"
              }
            }
          },
          {
            "kind": "GridLayoutItem",
            "spec": {
              "x": 0,
              "y": 24,
              "width": 12,
              "height": 8,
              "element": {
                "kind": "ElementReference",
                "name": "panel-7"
              }
            }
          },
          {
            "kind": "GridLayoutItem",
            "spec": {
              "x": 12,
              "y": 24,
              "width": 12,
              "height": 8,
              "element": {
                "kind": "ElementReference",
                "name": "panel-8"
              }
            }
          },
          {
            "kind": "GridLayoutItem",
            "spec": {
              "x": 0,
              "y": 32,
              "width": 12,
              "height": 8,
              "element": {
                "kind": "ElementReference",
                "name": "panel-9"
              }
            }
          },
          {
            "kind": "GridLayoutItem",
            "spec": {
              "x": 12,
              "y": 32,
              "width": 12,
              "height": 8,
              "element": {
                "kind": "ElementReference",
                "name": "panel-10"
              }
            }
          }
        ]
      }
    },
    "links": [],
    "liveNow": false,
    "preload": false,
    "tags": [],
    "timeSettings": {
      "timezone": "browser",
      "from": "now-30m",
      "to": "now",
      "autoRefresh": "",
      "autoRefreshIntervals": [
        "5s",
        "10s",
        "30s",
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "1d"
      ],
      "hideTimepicker": false,
      "fiscalYearStartMonth": 0
    },
    "title": "Celery-RabbitMQ-Redis Dashboard",
    "variables": [],
    "preferences": {
      "layout": {
        "kind": "GridLayout",
        "spec": {
          "items": []
        }
      }
    }
  }
}
```