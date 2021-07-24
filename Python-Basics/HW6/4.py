# 4. Реализуйте базовый класс Car. У данного класса должны быть следующие атрибуты: speed,
# color, name, is_police (булево). А также методы: go, stop, turn(direction), которые 
# должны сообщать, что машина поехала, остановилась, повернула (куда). Опишите несколько 
# дочерних классов: TownCar, SportCar, WorkCar, PoliceCar. Добавьте в базовый класс метод 
# show_speed, который должен показывать текущую скорость автомобиля. Для классов TownCar
# и WorkCar переопределите метод show_speed. При значении скорости свыше 60 (TownCar) и 
# 40 (WorkCar) должно выводиться сообщение о превышении скорости.
# Создайте экземпляры классов, передайте значения атрибутов. Выполните доступ к атрибутам, 
# выведите результат. Выполните вызов методов и также покажите результат.

class Car:
    
    def __init__(self,speed, color, name, is_police=False):
        self.speed = speed
        self.color = color
        self.name = name
        self.is_police = is_police
    
    def go(self):
        print(f'{self.name} is moving')
    
    def stop(self):
        print(f'{self.name} has stopped')
    
    def turn(self, direction):
        print(f'{self.name} is turning to the {direction}')
        
    def show_speed(self):
        print(f'Current speed of {self.name} is {self.speed}')

class TownCar(Car):
    def show_speed(self):
        if self.speed>60:
            print(f'Speed limit of 40 is violated.'
                  f' Current speed of {self.name} is {self.speed}')
        else:
            print(f'Current speed of {self.name} is {self.speed}')

class SportCar(Car):
    pass

class WorkCar(Car):
    def show_speed(self):
        if self.speed>40:
            print(f'Speed limit of 40 is violated. '
                  f'Current speed of {self.name} is {self.speed}')
        else:
            print(f'Current speed of {self.name} is {self.speed}')

class PoliceCar(Car):
    pass


town_car = TownCar(90, 'Red', 'Lada')
sport_car = SportCar(200, 'Blue', 'Maserati')
work_car = WorkCar(38, 'Black', 'Truck')
police_car = PoliceCar(120, 'Blue and White', 'Renault', True)


town_car.go()
town_car.show_speed()
town_car.turn('left')

sport_car.go()
sport_car.show_speed()
sport_car.turn('left')
sport_car.turn('right')
sport_car.stop()

work_car.go()
work_car.show_speed()

police_car.go()
police_car.show_speed()
police_car.turn('left')
police_car.turn('left')
police_car.stop()
