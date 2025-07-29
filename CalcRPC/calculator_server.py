from xmlrpc.server import SimpleXMLRPCServer

class Calculator:
    def add(self, x, y):
        return x + y
    
    def subtract(self, x, y):
        return x - y
    
    def multiply(self, x, y):
        return x * y
    
    def divide(self, x, y):
        if y == 0:
            return "Error: Division by zero"
        return x / y

def main():
    # Create server
    server = SimpleXMLRPCServer(("localhost", 8000))
    
    # Register the Calculator class
    calculator = Calculator()
    server.register_instance(calculator)
    
    print("Calculator server is running on localhost:8000")
    server.serve_forever()

if __name__ == "__main__":
    main() 