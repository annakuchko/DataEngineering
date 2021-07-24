# 7. Создать (не программно) текстовый файл, в котором каждая строка должна содержать 
# данные о фирме: название, форма собственности, выручка, издержки.
# Пример строки файла: firm_1 ООО 10000 5000.
# Необходимо построчно прочитать файл, вычислить прибыль каждой компании, а также среднюю 
# прибыль. Если фирма получила убытки, в расчет средней прибыли ее не включать.
# Далее реализовать список. Он должен содержать словарь с фирмами и их прибылями, а также 
# словарь со средней прибылью. Если фирма получила убытки, также добавить ее в словарь 
# (со значением убытков).
# Пример списка: [{“firm_1”: 5000, “firm_2”: 3000, “firm_3”: 1000}, {“average_profit”: 2000}].
# Итоговый список сохранить в виде json-объекта в соответствующий файл.
# Пример json-объекта:
# [{"firm_1": 5000, "firm_2": 3000, "firm_3": 1000}, {"average_profit": 2000}]

# Подсказка: использовать менеджеры контекста.

import json
with open('firms.txt', 'r', encoding = 'utf-8') as f:
    profits=[]
    firms=[]
    for l in f:
        firm_data = l.strip().split()
        profit = float(firm_data[2])-float(firm_data[3])
        firms.append(firm_data[0])
        profits.append(profit)
    pos_profits = [x for x in profits if x>0]
    avg_profit = sum(pos_profits)/len(pos_profits)
    data = [dict(zip(firms, profits)), {'average_profit':avg_profit}]
    
    
    with open("firms.json", "w") as write_f:
        json.dump(data, write_f)
