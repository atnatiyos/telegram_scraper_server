from telethon.sync import TelegramClient
import re
import Database

from dotenv import load_dotenv
import os
load_dotenv()

client = TelegramClient('scrap_freelancer', os.getenv('api_id'), os.getenv('api_hash'))

unspecified = []
def Job_category(job_sector):
    job_sector = job_sector.lower()

    if job_sector in ["sales and promotion", "accounting and finance", "marketing and advertisement", "broker and case closer", "purchasing and procurement", "business and commerce"]:
        return "Business and Commerce"
    elif job_sector in ["secretarial and office management", "project management and administration", "janitorial and other office services", "shop and office attendant","human resource and talent management"]:
        return "Administrative and Office Management"
    elif job_sector in ["software design and development", "information technology", "installation and maintenance technician", "mechanical and electrical engineering", "chemical and biomedical engineering", "environmental and energy engineering", "aeronautics and aerospace","Architecture and urban planning"]:
        return "Technology and Engineering"
    elif job_sector in ["creative art and design", "fashion design", "multimedia content production", "media and communication", "documentation and writing services", "translation and transcription"]:
        return "Creative and Design"
    elif job_sector in ["health care", "psychiatry, psychology, and social work", "pharmaceutical", "veterinary","psychiatry psychology and social work"]:
        return "Healthcare and Wellness"
    elif job_sector in ["hospitality and tourism", "beauty and grooming", "food and drink preparation or service"]:
        return "Hospitality, Tourism, and Food Services"
    elif job_sector in ["logistic and supply chain", "transportation", "transportation and delivery"]:
        return "Logistics and Supply Chain"
    elif job_sector in ["teaching and tutor", "training and mentorship"]:
        return "Education and Training"
    elif job_sector in ["manufacturing and production", "woodwork and carpentry","clothing and textile"]:
        return "Manufacturing and Production"
    elif job_sector in ["customer service and care","event management and organization"]:
        return "Customer Service"
    elif job_sector in ["research and data analytics", "data mining and analytics"]:
        return "Research and Analysis"
    elif job_sector in ["agriculture", "horticulture", "livestock and animal husbandry"]:
        return "Agriculture and Livestock"
    elif job_sector in ["construction and civil engineering", "gardening and landscaping", "labor work and masonry"]:
        return "Construction and Maintenance"
    elif job_sector in ["entertainment", "law", "security and safety", "advisory and consultancy"]:
        return "Entertainment, Legal, and Miscellaneous"
    else:
        if job_sector not in unspecified:
            unspecified.append(str(job_sector))
        return "Other"

# # Example usage
# print(Job_category("Health Care"))


def extract(message):
    start_index = str(message).find("['") + 2

    #Find the index of the closing bracket ']'
    end_index = str(message).find("']", start_index)

    # Extract the text between the brackets
    extracted_text = str(message)[start_index:end_index]
    #print( extracted_text)
    return extracted_text

def message_id():
    query = 'select MAX(message_id) from Jobs'
    try:
        Database.cursor.execute(query)
        result = Database.cursor.fetchall()
        
        return result[0][0]
    except:
        return float('inf')

last_message_id = message_id()   
async def main():
    
    # Connect to the Telegram server
    await client.start()

    # Get the input entity of the channel
    channel = await client.get_entity('@freelance_ethio')

    #from_date = datetime(2023, 5, 5, 0, 0, 0, tzinfo=pytz.utc) 
    # Get the messages from the channel
    
    Jobs = {}


    async for message in client.iter_messages(channel):
        # if message.date.replace(tzinfo=pytz.utc) > from_date:
        

        if message.id > last_message_id:
            print(message.id,last_message_id)

            
            try:
                content = message.message
                if extract(str(re.findall(r'Job Title: ([^\n]*)', content))) == '':
                    continue
                def correct_location(location):
                    
                    if ',' in location:
                        location = location.split(',')[0]
                        print(location)
                        return location
                    else:
                        return location
                Jobs[message.id] = {
                'Job_category':Job_category(extract(str(re.findall(r'Job Sector: ([^\n]*)', content)))[1:].replace("_"," ")),
                'Applicant_needed':  extract(re.findall(r'Applicants Needed: ([^\n]*)', content))    ,
                'Job_title':extract(str(re.findall(r'Job Title: ([^\n]*)', content))),
                'Exprience_level':extract(str(re.findall(r'Experience Level: ([^\n]*)', content))),
                'Job_type':extract(str(re.findall(r'Job Type: ([^\n]*)', content))),
                'Job_location':correct_location(extract(str(re.findall(r'Work Location: ([^\n]*)', content)))),
                'Job_sector':extract(str(re.findall(r'Job Sector: ([^\n]*)', content)))[1:],
                'Vacancy':extract(str(re.findall(r'Vacancies: ([^\n]*)', content))),
                'Salary_Compensation':extract(str(re.findall(r'Salary/Compensation: ([^\n]*)', content))),
                'Deadline':extract(str(re.findall(r'Deadline: ([^\n]*)', content))),
                'Description':extract(str(re.findall(r'Description:\n(.*?)\n', content, re.DOTALL))).replace("'",""),
                'Time':message.date.strftime("%d/%m/%y"),
                }

                try:    
                    query = "insert into Jobs(message_id,Job_category,Job_sector,Job_title,Job_location,Exprience_level,vacancies,Required_applicant,Salary,Deadline,Discription,Time_of_post) Values( " + str(message.id) + ",'"+Jobs[message.id]['Job_category'] +"','"+Jobs[message.id]['Job_sector']+"',N'"+Jobs[message.id]['Job_title']+"','"+Jobs[message.id]['Job_location']+"','"+Jobs[message.id]['Exprience_level']+"','"+Jobs[message.id]['Vacancy']+"','"+Jobs[message.id]['Applicant_needed']+"','"+Jobs[message.id]['Salary_Compensation']+"','"+Jobs[message.id]['Deadline']+"',N'"+Jobs[message.id]['Description'][:255]+"','"+str(Jobs[message.id]['Time'])+"')"
                
                    
                    Database.cursor.execute(query)
                    Database.conn.commit()
                    
                    
                except Exception as e:
                    print(f"Error processing message {message.id}: {e}")
                    continue
                
                
                

            except Exception as e:   
                print(e)
                continue
        else:
            
            break

# Run the main function
with client:
    client.loop.run_until_complete(main())
