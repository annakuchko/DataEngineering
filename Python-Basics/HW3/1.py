# 1. Реализовать функцию, принимающую два числа (позиционные аргументы) и выполняющую их деление. 
# Числа запрашивать у пользователя, предусмотреть обработку ситуации деления на ноль.

def devide():
    x = float(input("Введите первое число"))
    y = float(input("Введите второе число"))
    if y==0:
        print('Делить на ноль нельзя')
        pass
    else:
        print(f'{x}/{y} = {x/y}')
devide()