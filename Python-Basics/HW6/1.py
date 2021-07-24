# 1. Создать класс TrafficLight (светофор) и определить у него один атрибут color (цвет) 
# и метод running (запуск). Атрибут реализовать как приватный. В рамках метода реализовать
# переключение светофора в режимы: красный, желтый, зеленый. Продолжительность первого 
# состояния (красный) составляет 7 секунд, второго (желтый) — 2 секунды, третьего 
# (зеленый) — на ваше усмотрение. Переключение между режимами должно осуществляться 
# только в указанном порядке (красный, желтый, зеленый). Проверить работу примера, 
# создав экземпляр и вызвав описанный метод.
# Задачу можно усложнить, реализовав проверку порядка режимов, и при его нарушении выводить
# соответствующее сообщение и завершать скрипт.

import time

def cycle(it):
    while True:
        for x in it:
            yield x

#Example with error            
class TrafficLight:
    
    def __init__(self):
        self.__color = {0:['Red',7], 1:['Yellow', 2], 2:['Green',5], 3:['Blue',2]}

    def running(self):
        col=cycle(self.__color)
        for i in col:
            if i>0:
                
                if self.__color[i-1][0]=='Red' and self.__color[i][0]!='Yellow':
                    raise ValueError("Wrong colour order")
                    
                elif self.__color[i-1][0]=='Yellow' and self.__color[i][0]!='Green':
                    raise ValueError("Wrong colour order")
                    
                elif self.__color[i-1][0]=='Green' and self.__color[i][0]!='Red':
                    raise ValueError("Wrong colour order")
                    
                else:
                    print(f'{self.__color[i][0]}... Wait for {self.__color[i][1]} sec')
                    time.sleep(self.__color[i][1])
            
            else:
                print(f'{self.__color[i][0]}... Wait for {self.__color[i][1]} sec')
                time.sleep(self.__color[i][1])


traffic_light = TrafficLight()
traffic_light.running()
