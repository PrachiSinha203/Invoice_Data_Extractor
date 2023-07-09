import logging
import os.path
import json
import csv
import pandas as pd
import PyPDF2
import zipfile
import shutil

from adobe.pdfservices.operation.auth.credentials import Credentials
from adobe.pdfservices.operation.exception.exceptions import ServiceApiException, ServiceUsageException, SdkException
from adobe.pdfservices.operation.pdfops.options.extractpdf.extract_pdf_options import ExtractPDFOptions
from adobe.pdfservices.operation.pdfops.options.extractpdf.extract_element_type import ExtractElementType
from adobe.pdfservices.operation.pdfops.options.extractpdf.extract_renditions_element_type import \
    ExtractRenditionsElementType
from adobe.pdfservices.operation.pdfops.options.extractpdf.table_structure_type import TableStructureType
from adobe.pdfservices.operation.execution_context import ExecutionContext
from adobe.pdfservices.operation.io.file_ref import FileRef
from adobe.pdfservices.operation.pdfops.extract_pdf_operation import ExtractPDFOperation

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

try:
    # get base path.
    base_path = os.path.dirname(os.path.abspath(__file__))

    # Initial setup, create credentials instance.
    credentials = Credentials.service_account_credentials_builder().from_file(base_path + "/pdfservices-api-credentials.json").build()

    # Create an ExecutionContext using credentials and create a new operation instance.
    execution_context = ExecutionContext.create(credentials)
    extract_pdf_operation = ExtractPDFOperation.create_new()

    # Making new data path
    folder_path = base_path + '/Source'

    # File addresses storing
    # Iterate over files in the folder
    for filename in os.listdir(folder_path):
        # Check if the path is a file
        if os.path.isfile(os.path.join(folder_path, filename)):
            print(filename)
            # Set operation input from a source file.
            source = FileRef.create_from_local_file(folder_path + '/' + filename)
            extract_pdf_operation.set_input(source)

            # Build ExtractPDF options and set them into the operation
            extract_pdf_options: ExtractPDFOptions = ExtractPDFOptions.builder() \
                .with_elements_to_extract([ExtractElementType.TEXT, ExtractElementType.TABLES]) \
                .with_table_structure_format(TableStructureType.CSV) \
                .build()
            extract_pdf_operation.set_options(extract_pdf_options)

            # Execute the operation.
            result: FileRef = extract_pdf_operation.execute(execution_context)

            # Save the result to the specified location.        
            result.save_as(base_path + "/output.zip")
            
            # Path of zip file
            zip_file_path = base_path + "/output.zip"

            # Open the zip folder
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                # Get a list of all the file names within the zip folder
                file_names = zip_ref.namelist()
                
                # Iterate over each file in the zip folder
                for file_name in file_names:
                    # Extract the file
                    zip_ref.extract(file_name)
                        

            ## Merging two CSVs so that we form basic csv file

            # Counting file numbers

            # Get the list of files in the folder
            files = os.listdir(base_path + '/tables')

            # Count the number of files
            num_files = len(files)

            # Paths to the CSV files
            if(num_files - 3 >= 0):
                taxes = pd.read_csv('./tables/fileoutpart' + chr(num_files + 48 - 1) +'.csv')
                column = taxes.columns
                if(len(column) == 2):
                    heading_csv_path = (base_path + '/tables/fileoutpart' + chr(num_files + 48 - 3) + '.csv')
                    data_csv_path = (base_path + '/tables/fileoutpart' + chr(num_files + 48 - 2) + '.csv')
                else:
                    heading_csv_path = (base_path + '/tables/fileoutpart' + chr(num_files + 48 - 2) + '.csv')
                    data_csv_path = (base_path + '/tables/fileoutpart' + chr(num_files + 48 - 1) + '.csv')
                output_csv_path = base_path + '/output.csv'
            else:
                heading_csv_path = (base_path + '/tables/fileoutpart' + chr(num_files + 48 - 2) + '.csv')
                data_csv_path = (base_path + '/tables/fileoutpart' + chr(num_files + 48 - 1) + '.csv')
                output_csv_path = base_path + '/output.csv'

            # Read the headings from the heading CSV file
            headings = []
            with open(heading_csv_path, 'r') as heading_file:
                csv_reader = csv.reader(heading_file)
                headings = next(csv_reader)

            # Read the data from the data CSV file
            data = []
            with open(data_csv_path, 'r') as data_file:
                csv_reader = csv.reader(data_file)
                data = list(csv_reader)

            # Insert the headings into the data list
            data.insert(0, headings)

            # Write the merged data to the output CSV file
            with open(output_csv_path, 'w', newline='') as output_file:
                csv_writer = csv.writer(output_file)
                csv_writer.writerows(data)

            ## Entering extra information through json file

            # Reading all the datas again
            data1 = pd.read_csv('output.csv')
            search = base_path + '/structuredData.json'
            data2 = json.loads(open(search).read())
            
            # Assinging variables required
            i = 0
            flag = 1
            address = ""
            counter = 0
            mark_motto = counter
            mark_date = counter
            mark_name = counter
            mark_email = counter
            mark_phone = counter
            mark_add = counter
            useremail = ''
            user_address = ''
            row = 0
            col = 0
            id = 0
            invoiceID = ''

            # Iteration through json files for collecting extra info
            for value in data2['elements']:
                if 'attributes' in value: 
                    for key in value['attributes']:
                        if 'NumRow' in key: row = value['attributes']['NumRow']
                        if 'NumCol' in key: col = value['attributes']['NumCol']

                if 'Text' in value:
                    v = value['Text']
                    x = v.split()

                    #First thing in the document is shop's name
                    if (i == 0): shop_name = v

                    # the next thing read is address after which invoice is read
                    if(x[0] == 'Invoice#'):
                        if(len(x) > 1): invoiceID = x[0] + " " + x[1]
                        else:
                            invoiceID = x[0] + " "
                            id = i + 1
                        flag = 0
                        if(len(x) != 2): 
                            if(len(x) <= 4) : mark_date = i + 1
                            else: issue_date = x[4]
                        else: mark_date = i + 2
                    if(id == i): invoiceID = invoiceID + x[0]
                    # Until we get the invoice, we will be reading the address of the business
                    else:
                        if(flag == 1): 
                            if(v != shop_name): address = address + v

                    # Searches for issue date
                    if(i == mark_date):
                        if(mark_date != 0):
                            issue_date = x[len(x) - 1]

                    # Searches for due date
                    for k in range(len(x) - 2):
                        if(x[k] == 'Due'): due_date = x[k + 2]

                    # Searching description
                    if(i != 0):
                        if(v == shop_name): mark_motto = i + 1
                    if(i == mark_motto): motto = v

                    # Customer's info
                    if(x[0] == 'BILL'):
                        counter = 1
                        if(col == 0):
                            mark_name = i + 1
                        else:
                            mark_name = i + 6
                        mark_email = mark_name + 1
                        mark_phone = mark_name + 3
                        mark_add = mark_name + 4
                    if(i != 0):
                        if(i != mark_motto):
                            if(i == mark_name): name = v
                            if(i == mark_email): useremail = v
                            if(i == mark_email + 1) :

                                #Checking if EmailID Exists in next line or not
                                ans = x[len(x) - 1]
                                if(ans[-1] != 'm'): 
                                    mark_phone = mark_email + 1
                                    mark_add = mark_email + 2
                                else: useremail = useremail + v

                            if(i == mark_phone): phone = v
                            if(i == mark_add): user_address = v
                            if(i == mark_add + 1) : user_address = user_address + v
                i = i + 1

            # Correction in email-ID
            user = useremail.split()
            if(len(user) > 1): useremail = user[0] + user[1]

            # Correction in address
            add = address.split()
            size = len(add)
            street = ""
            for k in range(size - 4):
                street = street + " " + add[k]
            k = size - 4
            city = add[k]
            country = add[k + 1] + " " + add[k + 2]
            pincode = add[k + 3]

            # Taxes Accessing
            if(num_files - 3 >= 0):
                if(len(column) == 2):
                    data1['Invoice_Tax'] = taxes.iloc[0,1]
                else: data1['Invoice_Tax'] = ""
            else: data1['Invoice_Tax'] = ""
            

            # Data compiling
            data1['Invoice_ID'] = invoiceID
            data1['Issue_Date'] = issue_date
            data1['Due_Date'] = due_date
            data1['Customer_Name'] = name
            data1['Customer_EmailID'] = useremail
            data1['Customer_Phone'] = phone
            data1['Customer_Address'] = user_address
            data1["Business_Street"] = street
            data1["Business_City"] = city
            data1["Business_Country"] = country
            data1["Business_ZipCode"] = pincode
            data1['Business_Name'] = shop_name
            data1['Business_Description'] = motto

            # Removing the final output of the pdf
            os.remove(base_path + '/output.csv')
            os.remove(base_path + '/structuredData.json')
            # Delete the folder and its contents
            shutil.rmtree(base_path + '/tables')
            if os.path.isfile(zip_file_path): os.remove(zip_file_path)

            # Saving the final output
            csv_file = base_path + '/answer.csv'
            if (os.path.isfile(csv_file) == False):
                data1.to_csv('answer.csv', index = False)
            else:
                data1.to_csv('curr.csv', index = False)
                # Extracted CSV file paths
                csv_file_path1 = base_path + '/curr.csv'
                csv_file_path2 = base_path + '/answer.csv'
                # Read the CSV files into pandas DataFrames
                df1 = pd.read_csv(csv_file_path1)
                df2 = pd.read_csv(csv_file_path2)
                # Merge the DataFrames
                merged_df = pd.concat([df2, df1], ignore_index=True)
                merged_df.to_csv('updated.csv', index=False)
                # Deleting unnecessary files
                os.remove(csv_file_path1)
                os.remove(csv_file_path2)
                # New file name
                new_file_name = 'answer.csv'
                # Rename the file
                os.rename(base_path + '/updated.csv', new_file_name)


except (ServiceApiException, ServiceUsageException, SdkException):
    logging.exception("Exception encountered while executing operation")