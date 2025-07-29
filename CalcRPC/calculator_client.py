import xmlrpc.client

def main():
    # Connect to the server
    server = xmlrpc.client.ServerProxy("http://localhost:8000")
    
    while True:
        print("\nCalculator Menu:")
        print("1. Add")
        print("2. Subtract")
        print("3. Multiply")
        print("4. Divide")
        print("5. Exit")
        
        choice = input("Enter your choice (1-5): ")
        
        if choice == '5':
            print("Goodbye!")
            break
            
        if choice not in ['1', '2', '3', '4']:
            print("Invalid choice! Please try again.")
            continue
            
        try:
            num1 = float(input("Enter first number: "))
            num2 = float(input("Enter second number: "))
            
            if choice == '1':
                result = server.add(num1, num2)
                print(f"Result: {num1} + {num2} = {result}")
            elif choice == '2':
                result = server.subtract(num1, num2)
                print(f"Result: {num1} - {num2} = {result}")
            elif choice == '3':
                result = server.multiply(num1, num2)
                print(f"Result: {num1} * {num2} = {result}")
            elif choice == '4':
                result = server.divide(num1, num2)
                print(f"Result: {num1} / {num2} = {result}")
                
        except ValueError:
            print("Invalid input! Please enter numbers only.")
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 