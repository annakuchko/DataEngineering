# 3. Пользователь вводит месяц в виде целого числа от 1 до 12. Сообщить к какому 
# времени года относится месяц (зима, весна, лето, осень). Напишите решения через 
# list и через dict.

month = abs(int(input('Input month number from 1 to 12')))
season_list = ['winter','winter','winter', 
               'spring','spring','spring', 
               'summer','summer', 'summer',
               'autumn','autumn','autumn']
#print(season_list)
print((f'Season is {season_list[month]}'))

season_dict = {1:"winter",2:"winter",3:"winter",
               4:'spring',5:'spring',6:'spring',
               7:'summer',8:'summer',9:'summer',
               10:'autumn',11:'autumn',12:'autumn'}
print(f'Season is {season_dict[month]}')
