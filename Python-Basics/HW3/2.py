# 2. Реализовать функцию, принимающую несколько параметров, описывающих данные пользователя: 
# имя, фамилия, год рождения, город проживания, email, телефон. Функция должна принимать параметры
# как именованные аргументы. Реализовать вывод данных о пользователе одной строкой.

def user_data(name, surname, year_of_birth, city, email, phone):
    data = str(f'User: {name} {surname}, '
               f'Year of birth: {year_of_birth}, '
               f'City: {city}, Email: {email}, '
               f'Phone number: {phone}')
    #print(data)
    return data

user_data('Vasily', 'Pupkin', '1999', 'Karaganda', 'vaspupkin.99@mail.ru', '+78465798590')
