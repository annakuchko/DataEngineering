# 3. Создать текстовый файл (не программно), построчно записать фамилии сотрудников и 
# величину их окладов. Определить, кто из сотрудников имеет оклад менее 20 тыс., вывести 
# фамилии этих сотрудников. Выполнить подсчет средней величины дохода сотрудников.

with open('wages.txt', encoding = 'utf-8') as f:
    lines = f.readlines()
    w=[]
    for l in lines:
        ls = l.split(',')
        if float(ls[1])<20000:
            print(ls[0])
            w.append(ls[1].strip())
    print(f'Средняя зарплата: {sum(map(float, w))/len(w)}')
