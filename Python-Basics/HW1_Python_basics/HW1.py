#1. Поработайте с переменными, создайте несколько, выведите на экран, запросите у пользователя 
#несколько чисел и строк и сохраните в переменные, выведите на экран.

var1 = 'abc'
var2 = 12
print(var1*var2)

var3 = int(input('Input number: '))
var4 = input('Input string: ')
print(f'Your number: {var3}, your string: {var4}')

#2. Пользователь вводит время в секундах. Переведите время в часы, минуты и секунды и выведите 
# в формате чч:мм:сс. Используйте форматирование строк.

stime=int(input('Enter time in seconds: '))
h = stime//(60*60)
m = (stime-h*60*60)//60
s = (stime-h*60*60-m*60)
print(f'Time is {h}:{m}:{s:0=2d}')

# 3. Узнайте у пользователя число n. Найдите сумму чисел n + nn + nnn. Например, пользователь ввёл 
# число 3. Считаем 3 + 33 + 333 = 369.

n = input('Enter your number: ')
print(f'{n} + {2*n} + {3*n} = ', int(n)+int(2*n)+int(3*n))

# 4. Пользователь вводит целое положительное число. Найдите самую большую цифру в числе. Для решения 
# используйте цикл while и арифметические операции.

n = input('Enter positive natural number: ')
print(f'Maximum digit in number is: {max(list(map(int, list(n))))}')

# 5. Запросите у пользователя значения выручки и издержек фирмы. Определите, с каким финансовым 
# результатом работает фирма (прибыль — выручка больше издержек, или убыток — издержки больше выручки). 
# Выведите соответствующее сообщение. Если фирма отработала с прибылью, вычислите рентабельность выручки
# (соотношение прибыли к выручке). Далее запросите численность сотрудников фирмы и определите прибыль фирмы 
# в расчете на одного сотрудника.

revenue = float(input('Enter revenue: '))
costs = float(input('Enter costs: '))
income = revenue-costs
if income>0:
    ror = income/revenue
    print(f'Entity operates at a profit. Return on revenue is {round(ror,2)}')
    empl = int(input('Enter the number of employees: '))
    ipc = income/empl
    print(f'Income per capita is {round(ipc,2)}')
else:
    print('Entity operates at a loss')

# 6. Спортсмен занимается ежедневными пробежками. В первый день его результат составил a километров. 
# Каждый день спортсмен увеличивал результат на 10 % относительно предыдущего. Требуется определить номер 
# дня, на который общий результат спортсмена составить не менее b километров. Программа должна принимать 
# значения параметров a и b и выводить одно натуральное число — номер дня.
# Например: a = 2, b = 3.
# Результат:
# 1-й день: 2
# 2-й день: 2,2
# 3-й день: 2,42
# 4-й день: 2,66
# 5-й день: 2,93
# 6-й день: 3,22

# Ответ: на 6-й день спортсмен достиг результата — не менее 3 км.

a = float(input('Enter the first-day distance  (a): '))
b =  float(input('Enter the desired distance (b): '))
i=1
while a<b:
    a+=a*0.1
    i+=1
    print(f'Day {i}, distance {round(a,1)}km')
print(f'The sportsman has covered at least {b}km on day {i}')