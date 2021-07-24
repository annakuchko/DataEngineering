# 5. Реализовать класс Stationery (канцелярская принадлежность). Определить в нем атрибут
# title (название) и метод draw (отрисовка). Метод выводит сообщение “Запуск отрисовки.” 
# Создать три дочерних класса Pen (ручка), Pencil (карандаш), Handle (маркер). В каждом 
# из классов реализовать переопределение метода draw. Для каждого из классов методы должен 
# выводить уникальное сообщение. Создать экземпляры классов и проверить, что выведет
# описанный метод для каждого экземпляра.

class Stationery:
    
    def __init__(self, title):
        self.title = title
    
    def draw(self):
        print('Start drawing...')
    
class Pen(Stationery):
    def draw(self):
        print('Start drawing with pen...')

class Pencil(Stationery):
    def draw(self):
        print('Start drawing with pencil...')

class Handle(Stationery):
    def draw(self):
        print('Start drawing with handle...')
        
pen = Pen('Hello World!')
pencil = Pencil('Hello World!')
handle = Handle('Hello World!')

pen.draw()
pencil.draw()
handle.draw()
