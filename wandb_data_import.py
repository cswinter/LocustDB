import locustdb
import time
import wandb
import requests

entity = "entity-neural-network"
project = "enn-ppo"
run_id = "220511-055353-xor-num_envs=256-ent_coef=0.003-dmodel=64-anneal_ent=false-bs=1024-lr=0.0003"

api = wandb.Api(timeout=300)
runs = api.runs(f"{entity}/{project}", {
    'config.name': {"$regex": '220511-055353-.*'},
    # {
    #     "$text": '220511-055353',
    # }
})

print("Starting...")
i = 0
while True:
    try:
        run = next(runs)
    except requests.exceptions.HTTPError as e:
        print(e)
        continue
    except requests.exceptions.ReadTimeout as e:
        print(e)
        continue
    except StopIteration:
        break
    print(i, run.name)
    rows = 0
    for row in run.history(pandas=False):
        clean_row = {k: v or 0.0 for k, v in row.items() if not isinstance(v, dict) and not isinstance(v, str)}
        # print(clean_row)
        locustdb.log(table=run.name, metrics=clean_row)
        rows += 1
    print(f"Logged {rows} rows")
    i += 1

print("done")

# run = api.run(f"{entity}/{project}/{run_id}")

# # random walk
# print("starting logging...")
# value = 0
# for i in range(10000):
#     value += np.random.normal()
#     locustdb.log(table="test_metrics", metrics={"step": i, "cpu": value})

time.sleep(2)
print("done!")
