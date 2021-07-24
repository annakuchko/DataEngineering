# 1. Создать программно файл в текстовом формате, записать в него построчно данные, вводимые 
# пользователем. Об окончании ввода данных свидетельствует пустая строка.

f = open('file.txt', 'w')
while True:
    input_str = input("Enter values: ")
    if not input_str:
        break
    f.write(input_str+str('\n'))
f.close()
