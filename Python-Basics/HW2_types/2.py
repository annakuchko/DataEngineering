# 2. Для списка реализовать обмен значений соседних элементов, т.е. Значениями 
# обмениваются элементы с индексами 0 и 1, 2 и 3 и т.д. При нечетном количестве 
# элементов последний сохранить на своем месте. Для заполнения списка элементов 
# необходимо использовать функцию input().

my_list = (input('Input elements of list separated by comma')).split(',')
if len(my_list)//2==0:
    r = len(my_list)
else:
    r = len(my_list)-1
for i in range(0,r,2):
    my_list[i], my_list[i+1] = my_list[i+1], my_list[i]
print(my_list)
