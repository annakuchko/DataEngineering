# 4. Пользователь вводит строку из нескольких слов, разделённых пробелами. Вывести
# каждое слово с новой строки. Строки необходимо пронумеровать. Если в слово 
# длинное, выводить только первые 10 букв в слове.

words = input('Input string of multiple words separated by spaces').split(' ')
for i, w in enumerate(words):
    print(f'{i}  {w[:10]}')
    
