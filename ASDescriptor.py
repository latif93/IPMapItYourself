class ASDescriptor():
    def __init__(self, AS, type):
        self.AS = AS
        self.type = type
    
    def get_description(self):
        return self.AS, self.type