# 6. Реализовать функцию int_func(), принимающую слово из маленьких латинских букв и возвращающую 
# его же, но с прописной первой буквой. Например, print(int_func(‘text’)) -> Text.

def int_func(word):
    return (word.capitalize())

print(int_func('text'))

# Продолжить работу над заданием. В программу должна попадать строка из слов, разделенных пробелом.
# Каждое слово состоит из латинских букв в нижнем регистре. Сделать вывод исходной строки, но каждое
# слово должно начинаться с заглавной буквы. Необходимо использовать написанную ранее функцию int_func().

print(list(map(lambda x: int_func(x),input('Введите строку чисел через пробел').split(' '))))
