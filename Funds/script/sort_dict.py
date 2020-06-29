datas = [{"value": 4}, {"value": 1}, {"value": 2}, {"value": 3}]
ret = sorted(datas, key=lambda x: x["value"], reverse=True)
print(ret)
