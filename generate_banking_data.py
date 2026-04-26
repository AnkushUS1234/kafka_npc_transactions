#!/usr/bin/env python3
import json
import random
from datetime import datetime, timedelta

# Lists for generating realistic banking data
first_names = ["John", "Sarah", "Michael", "Emily", "David", "Lisa", "James", "Jennifer", "Robert", "Maria",
               "William", "Patricia", "Charles", "Linda", "Christopher", "Nancy", "Daniel", "Karen", "Paul", "Carol",
               "Mark", "Barbara", "Steven", "Dorothy", "Peter", "Sandra", "Andrew", "Jessica", "Joshua", "Angela",
               "Kenneth", "Melissa", "Kevin", "Donna", "Brian", "Carol", "George", "Barbara", "Edward", "Elizabeth",
               "Ronald", "Susan", "Anthony", "Margaret", "Frank", "Dorothy", "Ryan", "Ashley", "Gary", "Kimberly"]

last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
              "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
              "Lee", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Young", "Allen",
              "King", "Wright", "Scott", "Torres", "Peterson", "Phillips", "Campbell", "Parker", "Evans", "Edwards",
              "Collins", "Reeves", "Grant", "Hunter", "Gould", "Pena", "Beck", "Newman", "Haynes", "McDaniel"]

account_types = ["Checking", "Savings", "Money Market", "Business Checking", "Student Savings"]
bank_branches = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego",
                 "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "San Francisco",
                 "Indianapolis", "Seattle", "Denver", "Boston", "Washington", "Miami", "Atlanta", "Las Vegas", "Portland"]
states = ["NY", "CA", "TX", "IL", "AZ", "PA", "FL", "GA", "NC", "OH", "CO", "WA", "MA", "UT", "NV", "OR", "MI", "TN", "MO", "MD"]

transaction_types = ["Deposit", "Withdrawal", "Transfer", "Debit Card", "Bill Pay", "Fee", "Interest"]
descriptions = [
    "Direct Deposit - Payroll",
    "ATM Withdrawal",
    "Grocery Store Purchase",
    "Gas Station Purchase",
    "Restaurant Purchase",
    "Online Shopping",
    "Utility Bill Payment",
    "Loan Payment",
    "Transfer to Savings",
    "Transfer from Savings",
    "Check Deposit",
    "Mobile Payment",
    "Insurance Payment",
    "Rent Payment",
    "Subscription Service",
    "Dividend Payment",
    "Investment Purchase",
    "College Fund",
    "Emergency Withdrawal",
    "Business Expense",
    "Entertainment",
    "Hotel Booking",
    "Flight Booking",
    "Medical Payment",
    "Tax Refund"
]

def generate_streaming_banking_records(total_transactions, output_file):
    """Generate banking records for streaming - one row per transaction"""
    
    with open(output_file, 'w') as f:
        base_date = datetime(2024, 1, 1)
        account_id = 10000
        accounts_map = {}  # Store account info to reuse
        max_accounts = max(1, total_transactions // 3)  # Average 3 transactions per account
        
        for transaction_id in range(1, total_transactions + 1):
            # Randomly assign to existing or new account (some accounts will have multiple transactions)
            if len(accounts_map) < max_accounts and random.random() > 0.6:
                # Create new account (40% chance)
                account_id += 1
                first_name = random.choice(first_names)
                last_name = random.choice(last_names)
                account_type = random.choice(account_types)
                bank_branch = random.choice(bank_branches)
                state = random.choice(states)
                account_balance = round(random.uniform(1000, 50000), 2)
                
                accounts_map[account_id] = {
                    "account_holder": f"{first_name} {last_name}",
                    "account_type": account_type,
                    "bank_branch": bank_branch,
                    "state": state,
                    "account_balance": account_balance
                }
            else:
                # Reuse existing account (60% chance)
                if accounts_map:
                    account_id = random.choice(list(accounts_map.keys()))
                else:
                    # First transaction, create first account
                    account_id = 10001
                    first_name = random.choice(first_names)
                    last_name = random.choice(last_names)
                    account_type = random.choice(account_types)
                    bank_branch = random.choice(bank_branches)
                    state = random.choice(states)
                    account_balance = round(random.uniform(1000, 50000), 2)
                    
                    accounts_map[account_id] = {
                        "account_holder": f"{first_name} {last_name}",
                        "account_type": account_type,
                        "bank_branch": bank_branch,
                        "state": state,
                        "account_balance": account_balance
                    }
            
            # Get account details
            account_info = accounts_map[account_id]
            
            # Generate transaction
            transaction_amount = round(random.uniform(25, 5000), 2)
            transaction_type = random.choice(transaction_types)
            days_offset = transaction_id % 365
            transaction_date = (base_date + timedelta(days=days_offset)).strftime("%Y-%m-%d")
            description = random.choice(descriptions)
            
            # Create streaming record - one transaction per row
            record = {
                "transaction_id": transaction_id,
                "account_id": account_id,
                "account_holder": account_info["account_holder"],
                "account_type": account_info["account_type"],
                "bank_branch": account_info["bank_branch"],
                "state": account_info["state"],
                "account_balance": account_info["account_balance"],
                "transaction_amount": transaction_amount,
                "transaction_type": transaction_type,
                "transaction_date": transaction_date,
                "description": description
            }
            
            f.write(json.dumps(record) + '\n')
            
            if transaction_id % 5000 == 0:
                print(f"Generated {transaction_id} transactions...")
        
        print(f"\nGeneration complete!")
        print(f"Total transactions (unique rows): {total_transactions}")
        print(f"Total unique accounts: {len(accounts_map)}")

if __name__ == "__main__":
    output_file = "/Users/Ankush/Downloads/CONFLUENT_KAFKA/banking_input.json"
    total_transactions = 57430
    
    print(f"Generating {total_transactions} banking transactions for streaming (one per row)...")
    generate_streaming_banking_records(total_transactions, output_file)
    print(f"\n✓ Successfully generated banking_input.json")