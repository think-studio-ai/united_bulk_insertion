from datetime import datetime
import pandas as pd



def extract_info_from_national_id(national_id):
    # Extract birthdate parts
    try:
        national_id = str(int(national_id))
        century_code = int(national_id[0])
        year_of_birth = int(national_id[1:3])
        month_of_birth = int(national_id[3:5])
        day_of_birth = int(national_id[5:7])

        # Determine century and correct year of birth
        if century_code == 2:
            year_of_birth += 1900
        elif century_code == 3:
            year_of_birth += 2000
        else:
            return national_id

        # Validate month and day ranges
        if not (1 <= month_of_birth <= 12):
            return national_id

        # Handle invalid days based on month
        try:
            birthdate = datetime(year_of_birth, month_of_birth, day_of_birth)
        except ValueError:
            return national_id

        # Gender determination
        gender_code = int(national_id[12])
        gender = 'Male' if gender_code % 2 != 0 else 'Female'

        # Calculate age
        today = datetime.today()
        age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))

        # Return results
        return {
            'birthdate': birthdate.strftime('%Y-%m-%d'),
            'age': age,
            'gender': gender
        }
    except Exception:
        return national_id

def validate_column(df, required_columns):
    columns_list = list(df.columns)
    for column in columns_list:
        if column not in required_columns:
            return False
        return True

def validate_column_unique_values(df, column_name):
    unique_values = df[column_name].unique()
    if len(unique_values) > 1:
        return False
    return True

def validate_gov_column(df, gov_list):
    invalid_gov_values = df[~df['gov'].isin(gov_list)]['gov'].unique()
    if len(invalid_gov_values) > 0:
        return False
    return True


def collect_services(row):
    services = []
    for i in range(4):
        service_key = f'serviceProvided{i}' if i != 0 else 'serviceProvided'
        date_key = f'serviceProvidedDate{i}' if i != 0 else 'serviceProvidedDate'

        service_date = row.get(date_key, None)

        service_value = row.get(service_key)
        service = str(service_value).strip() if service_value else None

        association_value = row.get('association')
        association = str(
            association_value).strip() if association_value else None

        if pd.notna(service):
            services.append({
                "service": service,
                "date": service_date if pd.notna(service_date) else None,
                "association": association if pd.notna(association) else None
            })

    return services
