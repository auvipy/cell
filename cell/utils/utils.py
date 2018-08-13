
def lazy_property(property_name, property_factory, doc=None):

    def get(self):
        if not hasattr(self, property_name):
            setattr(self, property_name, property_factory(self))
        return getattr(self, property_name)
    return property(get)
