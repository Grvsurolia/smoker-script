import mysql.connector
from mysql.connector import errorcode
import settings
from prettytable import PrettyTable
import matplotlib.pyplot as plt
import pandas as pd
import time

# Function to create a connection to the MySQL database
def connect():
    try:
        conn = mysql.connector.connect(
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            host=settings.DB_HOST
        )

        # Check if the database exists, and create it if it doesn't
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {settings.DB_NAME}")
        cursor.close()

        # Connect to the specified database
        conn = mysql.connector.connect(
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            host=settings.DB_HOST,
            database=settings.DB_NAME
        )
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Access denied. Check your username or password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(err)
        return None

def create_tables(conn):
    cursor = conn.cursor()

    try:
        # Smokers table (create this table first)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Smokers (
                smoker_id INT AUTO_INCREMENT PRIMARY KEY,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                date_of_birth DATE,
                gender VARCHAR(20),
                contact_information VARCHAR(255)
            )
        """)
        # print("Smokers table created successfully.")

        # Smoking_Habits table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Smoking_Habits (
                habit_id INT AUTO_INCREMENT PRIMARY KEY,
                smoker_id INT,
                start_date DATE,
                quit_date DATE,
                cigarettes_per_day INT,
                pack_years INT,
                smoking_status VARCHAR(20),
                INDEX (smoker_id),  -- Add an index on smoker_id for optimization
                FOREIGN KEY (smoker_id) REFERENCES Smokers(smoker_id)
            )
        """)
        # print("Smoking_Habits table created successfully.")

        # Health_Records table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Health_Records (
                record_id INT AUTO_INCREMENT PRIMARY KEY,
                smoker_id INT,
                FOREIGN KEY (smoker_id) REFERENCES Smokers(smoker_id),
                record_date DATE,
                health_condition VARCHAR(255),
                diagnosis_date DATE,
                treatment_history TEXT,
                severity VARCHAR(20)
            )
        """)
        # print("Health_Records table created successfully.")

        # Demographics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Demographics (
                smoker_id INT,
                FOREIGN KEY (smoker_id) REFERENCES Smokers(smoker_id),
                education_level VARCHAR(255),
                income_level VARCHAR(255),
                employment_status VARCHAR(255),
                ethnicity VARCHAR(255),
                location VARCHAR(255)
            )
        """)
        # print("Demographics table created successfully.")

        conn.commit()
        print("All tables created successfully.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()


# Function to create a new smoker and associated records
def create_smoker(conn):
    cursor = conn.cursor()

    try:
        # Insert into Smokers table
        cursor.execute("""
            INSERT INTO Smokers
            (first_name, last_name, date_of_birth, gender, contact_information)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            input("Enter first name: "),
            input("Enter last name: "),
            input("Enter date of birth (YYYY-MM-DD): "),
            input("Enter gender: "),
            input("Enter contact information: ")
        ))

        smoker_id = cursor.lastrowid  # Get the ID of the last inserted row
        print()

        # Insert into Smoking Habits table
        cursor.execute("""
            INSERT INTO Smoking_Habits
            (smoker_id, start_date, quit_date, cigarettes_per_day, pack_years, smoking_status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            smoker_id,
            input("Enter start date of smoking (YYYY-MM-DD): "),
            input("Enter quit date (YYYY-MM-DD): "),
            int(input("Enter cigarettes per day: ")),
            int(input("Enter pack years: ")),
            input("Enter smoking status (e.g., current, former, never): ")
        ))
        print()

        # Insert into Health Records table
        cursor.execute("""
            INSERT INTO Health_Records
            (smoker_id, record_date, health_condition, diagnosis_date, treatment_history, severity)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            smoker_id,
            input("Enter record date (YYYY-MM-DD): "),
            input("Enter health condition: "),
            input("Enter diagnosis date (YYYY-MM-DD): "),
            input("Enter treatment history: "),
            input("Enter severity (e.g., mild, moderate, severe): ")
        ))
        print()

        # Insert into Demographics table
        cursor.execute("""
            INSERT INTO Demographics
            (smoker_id, education_level, income_level, employment_status, ethnicity, location)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            smoker_id,
            input("Enter education level: "),
            input("Enter income level: "),
            input("Enter employment status: "),
            input("Enter ethnicity: "),
            input("Enter location: ")
        ))

        conn.commit()
        print("Smoker and associated records created successfully.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()

# Function to get smoker information and associated records in tabular format
def get_smoker_by_phone_number(conn, phone_number):
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT * FROM Smokers
            INNER JOIN Smoking_Habits ON Smokers.smoker_id = Smoking_Habits.smoker_id
            INNER JOIN Health_Records ON Smokers.smoker_id = Health_Records.smoker_id
            INNER JOIN Demographics ON Smokers.smoker_id = Demographics.smoker_id
            WHERE Smokers.contact_information = %s
        """, (phone_number,))

        result = cursor.fetchall()
        if result:
            table = PrettyTable()
            table.field_names = result[0].keys()
            for row in result:
                table.add_row(row.values())
            print(table)
        else:
            print("Smoker not found.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()

# Function to get all records and display in tabular format
def get_all_records(conn):
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT * FROM Smokers
            INNER JOIN Smoking_Habits ON Smokers.smoker_id = Smoking_Habits.smoker_id
            INNER JOIN Health_Records ON Smokers.smoker_id = Health_Records.smoker_id
            INNER JOIN Demographics ON Smokers.smoker_id = Demographics.smoker_id
        """)

        result = cursor.fetchall()
        if result:
            table = PrettyTable()
            table.field_names = result[0].keys()
            for row in result:
                table.add_row(row.values())
            print(table)
        else:
            print("No records found.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()


def delete_smoker(conn, smoker_id):
    cursor = conn.cursor()

    try:
        # Check if the record exists
        cursor.execute("""
            SELECT 1 FROM Smokers WHERE smoker_id = %s
        """, (smoker_id,))
        
        if cursor.fetchone():
            # Delete from Demographics table first
            cursor.execute("""
                DELETE FROM Demographics WHERE smoker_id = %s
            """, (smoker_id,))

            # Delete from Health_Records table
            cursor.execute("""
                DELETE FROM Health_Records WHERE smoker_id = %s
            """, (smoker_id,))

            # Delete from Smoking_Habits table
            cursor.execute("""
                DELETE FROM Smoking_Habits WHERE smoker_id = %s
            """, (smoker_id,))

            # Now delete from Smokers table
            cursor.execute("""
                DELETE FROM Smokers WHERE smoker_id = %s
            """, (smoker_id,))

            conn.commit()
            print("Smoker and associated records deleted successfully.")
        else:
            print("No record found with this ID.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()




# Function to get all records and export to Excel
def export_all_records_to_excel(conn, excel_file):
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT * FROM Smokers
            INNER JOIN Smoking_Habits ON Smokers.smoker_id = Smoking_Habits.smoker_id
            INNER JOIN Health_Records ON Smokers.smoker_id = Health_Records.smoker_id
            INNER JOIN Demographics ON Smokers.smoker_id = Demographics.smoker_id
        """)

        result = cursor.fetchall()
        if result:
            df = pd.DataFrame(result)
            df.to_excel(excel_file, index=False)
            print(f"Data exported to {excel_file} successfully.")
        else:
            print("No records found.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()


# Function to display a bar chart of cigarettes smoked per day for each smoker
def display_cigarettes_per_day_chart(conn):
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT S.smoker_id, S.first_name, S.last_name, SH.cigarettes_per_day
            FROM Smokers S
            INNER JOIN Smoking_Habits SH ON S.smoker_id = SH.smoker_id
        """)

        result = cursor.fetchall()
        if result:
            smoker_ids = [f"{row['smoker_id']}" for row in result]
            cigarettes_per_day = [row['cigarettes_per_day'] for row in result]

            # Plotting the bar chart
            plt.bar(smoker_ids, cigarettes_per_day, color='skyblue')
            plt.xlabel('Smoker ID')
            plt.ylabel('Cigarettes per Day')
            plt.title('Cigarettes per Day for Each Smoker')

            # Adding the number of cigarettes on top of each bar
            for i, v in enumerate(cigarettes_per_day):
                plt.text(i, v + 0.1, str(v), ha='center', va='bottom')

            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.show()
        else:
            print("No records found.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()


# Function to get and display smoker information with dynamic sorting
def get_and_display_sorted_smokers(conn, sort_column='last_name', sort_order='ASC'):
    cursor = conn.cursor(dictionary=True)

    try:
        # Validate sort_order to prevent SQL injection
        sort_order = sort_order.upper()  # Convert to uppercase for case-insensitivity
        if sort_order not in ['ASC', 'DESC']:
            print("Invalid sort order. Using default order (ASC).")
            sort_order = 'ASC'

        # Modify the SQL query to include dynamic ORDER BY clause
        query = f"""
            SELECT * FROM Smokers
            INNER JOIN Smoking_Habits ON Smokers.smoker_id = Smoking_Habits.smoker_id
            INNER JOIN Health_Records ON Smokers.smoker_id = Health_Records.smoker_id
            INNER JOIN Demographics ON Smokers.smoker_id = Demographics.smoker_id
            ORDER BY {sort_column} {sort_order}
        """
        cursor.execute(query)

        result = cursor.fetchall()
        if result:
            table = PrettyTable()
            table.field_names = result[0].keys()
            for row in result:
                table.add_row(row.values())
            print(table)
        else:
            print("No records found.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()

# Example usage
conn = connect()
if conn:
    while True:
        msg = """
Hello! Please select one from below:
1. To create smoker data.
2. To get a single smoker record.
3. To get all smoker record.
4. To delete a smoker record.
5. To export all data into excel (smokers_data.xlsx)
6. To display cigarettes per day chart
7. To sort data
8. To Stop:\n\n"""
        time.sleep(1)
        n = int(input(msg))
        create_tables(conn)
        if n==1:
            create_smoker(conn)
        elif n==2:
            phone_number_to_get = input("Enter the phone number to retrieve information: ")
            get_smoker_by_phone_number(conn, phone_number_to_get)
        elif n==3:
            get_all_records(conn)
        elif n==4:
            smoker_id_to_delete = int(input("Enter the smoker_id to delete: "))
            delete_smoker(conn, smoker_id_to_delete)
        elif n==5:
            export_all_records_to_excel(conn, "smokers_data.xlsx")
        elif n==6:
            display_cigarettes_per_day_chart(conn)
        elif n==7:
            sort_column_input = input("Enter the column to sort by (default: last_name): (first_name, date_of_birth, contact_information):\n") or 'last_name'
            sort_order_input = input("Enter the sort order (ASC/DESC, default: ASC):\n") or 'ASC'
            
            get_and_display_sorted_smokers(conn, sort_column=sort_column_input, sort_order=sort_order_input)
        
        elif n==8:
            print("STOPING.....")
            time.sleep(1)
            conn.close()
            quit()
        else:
            print("Please choose valid selection.")
