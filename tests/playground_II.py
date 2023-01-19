

class MyClass:

    @property
    def my_dict(self):
        try:
            return getattr(self, '_my_dict')
        except AttributeError:
            setattr(self, '_my_dict', {})
        return getattr(self, '_my_dict')


obj = MyClass()

obj_my_dict = obj.my_dict

obj_my_dict['b'] = 2
obj.my_dict['a'] = 1

print(obj.my_dict)

