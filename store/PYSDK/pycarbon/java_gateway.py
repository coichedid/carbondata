from py4j.java_gateway import JavaGateway


class java_gateway(object):
    def __init__(self):
        self.gateway = JavaGateway()

    def get_java_entry(self):
        return self.gateway.entry_point
