import pandas as pd

# information about employees
id_number = ['128', '478', '257', '299', '175', '328', '099', '457', '144', '222']
name = ['Patrick', 'Amanda', 'Antonella', 'Eduard', 'John', 'Alejandra', 'Layton', 'Melanie', 'David', 'Lewis']
surname = ['Miller', 'Torres', 'Brown', 'Iglesias', 'Wright', 'Campos', 'Platt', 'Cavill', 'Lange', 'Bellow']
division = ['Sales', 'IT', 'IT', 'Sales', 'Marketing', 'Engineering', 'Engineering', 'Sales', 'Engineering', 'Sales']
salary = [30000, 54000, 80000, 79000, 15000, 18000, 30000, 35000, 45000, 30500]
telephone = ['7366578', '7366444', '7366120', '7366574', '7366113', '7366117', '7366777', '7366579', '7366441', '7366440']
type_contract = ['permanent', 'temporary', 'temporary', 'permanent', 'internship', 'internship', 'permanent', 'temporary', 'permanent', 'permanent']

# data frame containing information about employees
df_employees = pd.DataFrame({'name': name, 'surname': surname, 'division': division,
                             'salary': salary, 'telephone': telephone, 'type_contract': type_contract}, index=id_number)


print("DATAFRAME INFO")
print(df_employees.info())

print("==================================================================================")
print(">>>>>>>TOTAL COUNT: " + str(len(df_employees)))
print(df_employees)
print("==================================================================================")

f1 = df_employees[(df_employees['salary'] > 45000) & (df_employees['type_contract'] == 'permanent')]
print(">>>>>>> FILTERED 1: " + str(len(f1)))
print(f1)

f2 = df_employees[df_employees['type_contract'].isin(['temporary', 'permanent'])]
print(">>>>>>> FILTERED 2: "+ str(len(f2)) )
print(f2)