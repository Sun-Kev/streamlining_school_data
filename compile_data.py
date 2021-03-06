# Date of last edit: Thursday, 05.17.2018
#
# Author: Kevin Sun
#
# This compile_data.py file will take multiple csv or excel files, clean
# and reorganize the data into a single dataframe. Each row of the
# final dataframe will contain all relevant numbers and data points
# for a single student.


from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np
import pandas as pd
from collections import defaultdict

########## IMPORTANT ############
#								#
# WHEN NEW FILES ARE DOWNLOADED #
#      COPY AND PASTE THE       #
#      NEW FILENAMES BELOW	    #
#								#
#################################

### FILES IN THIS SECTION MUST BE UPDATED WEEKLY ###
# UPLOAD DATE IS: MAY 15, 2018
GPA_DATA = "week_gpa.csv" # updated May15
CLASS_RANK_DATA = "gpa_rank.csv" # updated Apr17 & Keep until EOY
WEEKLY_ATTN_DATA = "week_attn.csv" # updated May15
YTD_ATTN_DATA = "ytd_attn.csv" #updated May15
SWIPE_DATA = "swipe.csv" # updated May15
CURRENT_GRADES = "grades.csv" # updated May15

### FILES IN THIS SECTION ARE REUSED WEEK-TO-WEEK ###
EMAIL_LIST =  "emails.xls" #
SAT_9 = "sat_9.xlsx" # New scores coming May 20
SAT_10 = "sat_10.xlsx" # New scores coming May 20
SAT_11 = "sat_11.xlsx" # New scores coming May 20
#SAT_12 = "sat_12.xlsx" # issue: incorrect excel sheet
SL_11 = "sl_11.csv"
SL_12 = "sl_12.csv"
START_DATE = "May 14, 2018"
END_DATE = "May 18, 2018"

###########################
#    PLEASE DO NOT MAKE   #
#   CHANGES TO THE CODE   #
# 		   BELOW          #
###########################

######### STEP 1 ##########
#    IMPORT THE DATA      #
# FILTER RELEVANT COLUMNS #
#    THIS STEP HAS 12     #
#   RELEVANT FUNCTIONS	  #
###########################

def import_gpa_data(filename):
	"""
	This function takes csv file of STUDENT GPA DATA and 
	returns a cleaned pandas dataframe w/ columns renamed.

	Input:
		- filename: a string name of the csv file
	Output:
		- gpa_df: a pandas dataframe of the gpa data
	"""
	gpa_df = pd.read_csv(filename, index_col='STUDENT ID')
	gpa_df = gpa_df.rename(index=int, columns={"GRADE LEVEL": "grade", 
		"LAST NAME": "last_name", "FIRST NAME": "first_name", 
		"AVG GPA":"weekly_gpa"})
	gpa_df.index.names = ['ID']
	# round values to 2 decimal places
	gpa_df = gpa_df.round(2)

	return gpa_df


def import_rank_data(filename):
	"""
	This function takes csv file of STUDENT CLASS RANK DATA and 
	returns a cleaned pandas dataframe w/ columns renamed.

	Input:
		- filename: a string name of the csv file
	Output:
		- classrank_df: a pandas dataframe of the class rank data
	"""
	classrank_df = pd.read_csv(filename, header=None,
		usecols=[13,18,20,21,22], index_col=0)
	classrank_df = classrank_df.rename(index=int, columns={18: "unweight_gpa", 20: "class_rank",
		21: "class_size", 22: "credits_earned"})
	# round cum. unweight gpa to 2 decimal places
	classrank_df['unweight_gpa'] = classrank_df['unweight_gpa'].round(2)
	classrank_df.index.names = ['ID']

	return classrank_df


def import_week_attn_data(filename):
	"""
	This function takes csv file of STUDENT WEEKLY ATTENDANCE DATA and 
	returns a cleaned pandas dataframe w/ columns renamed.

	Input:
		- filename: a string name of the csv file
	Output:
		- weekly_df: a pandas dataframe of the weekly attendance data
	"""
	weekly_df = pd.read_csv(filename, usecols=['Student ID',
		'Week','Attendance Pct'], index_col='Student ID')
	weekly_df = weekly_df.rename(index=int, columns={"Week": "week",
		"Attendance Pct": "weekly_attn"})
	weekly_df.index.names = ['ID']
	
	return weekly_df


def import_year_attn_data(filename):
	"""
	This function takes csv file of STUDENT YTD ATTENDANCE DATA and 
	returns a cleaned pandas dataframe w/ columns renamed.

	Input:
		- filename: a string name of the csv file
	Output:
		- ytd_attn_df: a pandas dataframe of the year-to-date attendance data
	"""
	ytd_attn_df = pd.read_csv(filename, usecols=['Student ID',
		'Current School', 'Attendance Pct'], index_col='Student ID')
	# keep only active studenst and drop inactive students
	active = ytd_attn_df['Current School'] == "HYDE PARK HS"
	ytd_attn_df = ytd_attn_df[active]
	# drop Current School column
	ytd_attn_df = ytd_attn_df.drop(labels = "Current School", axis=1)
	ytd_attn_df = ytd_attn_df.rename(index=int, columns={"Attendance Pct"
		: "ytd_attn"})
	ytd_attn_df.index.names = ['ID']				
	
	return ytd_attn_df


def import_swipe_data(filename):
	"""
	This function takes csv file of STUDENT SWIPE DATA and 
	returns a cleaned pandas dataframe w/ columns renamed.

	Input:
		- filename: a string name of the csv file
	Output:
		- final_swipe_df: a pandas dataframe of the swipe data
	"""
	# initialize date dictionary
	date_dict = {'1':'Jan', '2':'Feb', '3':'Mar', '4':'Apr', '5':'May',
		'6':'Jun', '7':'Jul', '8':'Aug', '9':'Sept', '10':'Oct',
		'11':'Nov', '12':'Dec'}
	# read in swipe report data and rename columns
	swipe_df = pd.read_csv(filename, usecols=['Textbox20',
		'Textbox12', 'Textbox14', 'Type'])
	swipe_df = swipe_df.rename(index=int, columns={
		"Textbox20": "ID", "Textbox12": "date",
		"Textbox14": "swipe_time"})
	# keep only student ID number in ID column
	swipe_df['ID'] = swipe_df['ID'].apply(lambda x: int(x.split(" ")[0]))
	drop_list = []
	for row in swipe_df.itertuples():
		index, time = row[0], row[3]
		h, m, s = time.split(":")
		cut_off = int(h+m)
		# count only students swiping between 900-1030am
		if (cut_off < 900) or (cut_off > 1030):
			drop_list.append(index)
			swipe_df1 = swipe_df.drop(drop_list)
	swipe_df1.set_index('ID', inplace=True)
	# initialize defualt dictionaries to put indiv. data into single row
	d_date = defaultdict(list)
	d_time = defaultdict(list)
	for t in swipe_df1.itertuples():
		i, d, s, x = t
		# rename dates and append
		month = date_dict[d[0]]
		day = d.split('/')
		final_date = month + " " + day[1]
		d_date[i].append(final_date)
		# truncate times and append
		hrs, mins, sec = s.split(":")
		final_time = hrs + ":" + mins + " " + "AM"		
		d_time[i].append(final_time)
	# make values a string
	d_date = str_list(d_date)
	d_time = str_list(d_time)
	# put into data frames
	df_date = pd.DataFrame.from_dict(d_date, orient='index')
	df_time = pd.DataFrame.from_dict(d_time, orient='index')
	final_swipe_df = pd.merge(df_date, df_time, left_index=True, 
		right_index=True)
	# rename columns
	final_swipe_df = final_swipe_df.rename(index=int, 
		columns={"0_x": "late_date", "0_y":"late_time"})

	return final_swipe_df


def str_list(d):
	"""
	This is a helper function to turn the values of the keys of a
	python dictionary into a string. This is utilized for cleaning
	the swipes dataframe.
	"""
	d_new = {}
	for key, value in d.items():
		d_new[key] = str(value)

	return d_new

def import_sat(filenames):
	"""
	This function takes an excel files of SAT_9, SAT_10, SAT_11 and 
	returns a cleaned pandas dataframe w/ columns renamed.

	Input:
		- filename: a list of names of the excel file

	Output:
		- sat_df: a pandas dataframe of SAT score data
	"""
	# import SAT scores for each grade
	# l = []
	# for file in filenames:
	# 	df = pd.read_excel(file, skiprows=9, usecols=['Student ID', 'Total Score', 'ERW', 'Math'])
	# 	# drop students with missing IDs
	# 	df.dropna(inplace=True)
	# 	# make ID an integer and index
	# 	df['Student ID'] = df['Student ID'].apply(lambda x: int(x))
	# 	# rename columns 		
	# 	df = df.rename(index=int, columns={'Student ID':'ID', 'Total Score':'composite_sat', 
	# 					'Math':'math_sat', 'ERW':'erw_sat'})
	# 	# index by ID
	# 	df.set_index('ID')
	# 	l.append(df)

	# nine, ten, eleven = l
	# sat_df = nine.append([ten, eleven])
	# sat_df = sat_df.set_index('ID')

	# return sat_df

	# EDIT FOR NEW SAT SCORES #
	l = []
	for file in filenames:
		df = pd.read_excel(file, skiprows=9, usecols=['Student ID', 'Total Score', 'ERW', 'Math'])
		# drop students with missing IDs
		df.dropna(inplace=True)
		# make ID an integer and index
		df['Student ID'] = df['Student ID'].apply(lambda x: int(x))
		# rename columns 		
		df = df.rename(index=int, columns={'Student ID':'ID', 'Total Score':'composite_sat', 
						'Math':'math_sat', 'ERW':'erw_sat'})
		# index by ID
		df.set_index('ID')
		# fill each column with temporary info
		df['composite_sat'] = 'Scores coming in mid-May'
		df['math_sat'] = 'Scores coming in mid-May'
		df['erw_sat'] = 'Scores coming in mid-May'
		l.append(df)

	nine, ten, eleven = l
	sat_df = nine.append([ten, eleven])
	sat_df = sat_df.set_index('ID')

	return sat_df


def import_service_learning(filenames):
	"""
	This function takes multiple csv files of SERVICE LEARNING HOURS
	and returns a cleaned pandas dataframe w/ columsn renamed.
	
	Input:
		- filenames: a list of string names of csv files
	Output:
		- sl_df: a dataframe of 11th and 12th grade service learning
			hours
	"""
	l = []
	for file in filenames:
		df = pd.read_csv(file, header=None, usecols=[10, 12])
		df.rename(columns={10: 'ID', 12: 'service_hours'}, inplace=True)
		df.dropna(subset=['ID'], inplace=True)
		df['ID'] = df['ID'].astype(int) 
		df.set_index('ID', inplace=True)
		l.append(df)

	eleven, twelve = l
	sl_df = eleven.append(twelve)

	return sl_df


def import_current_grades(filename):
	"""
	This function takes a csv file of all student CURRENT GRADES
	and returns a cleaned pandas dataframe w/ columns renamed

	Input:
		- filename: a csv file
	Output:
		- curr_grade_df: a pandas dataframe of current grades
	"""
	# import relevant csv file columns into pandas dataframe
	cg_df = pd.read_csv(filename, usecols=['Student ID', 'Grade Level', 'Student Name',
		'Period', 'Course Name', 'CAvg'])
	
	# create dictionary that maps Student ID to Grade Level
	d = {}
	for row in cg_df.iterrows():
		# get the student ID number
		id_num = row[1][0]
		# get the student's grade level
		grade = row[1][2]
		if id_num not in d:
			d[id_num] = grade

	# collapse dataframe based on student ID, identify cols by Period, 
	# and fill in grade averages
	cg_df=cg_df.pivot(index='Student ID', columns='Period', values='CAvg').reset_index().set_index('Student ID')
	
	# rename the columns
	cg_df.rename(index=int, columns={'Student ID':'ID', '01 Per':'p1', 
		'02 Per':'p2', '03 Per':'p3', '04 Per':'p4', '05 Per':'p5',
		'06 Per':'p6', '07 Per':'p7', '08 Per':'p8', 'Grade Level': 'grade'}, inplace=True)

	# add the grade level column back in
	cg_df['grade_level'] = cg_df.index.to_series().map(d)

	# obtain sub-dataframes of each grade
	nine = cg_df['grade_level'] == 9
	ten = cg_df['grade_level'] == 10
	eleven = cg_df['grade_level'] == 11
	twelve = cg_df['grade_level'] == 12

	# fill NAs with appropirate Lunch Periods
	cg_df[nine].p3.fillna(value='Lunch', inplace=True)
	cg_df[ten].p4.fillna(value='Lunch', inplace=True)
	cg_df[eleven].p5.fillna(value='Lunch', inplace=True)

	# concatenate back into a single DataFrame
	#cg_df = pd.concat([df9, df10, df11, df12])

	# fill in Periods 3, 4, 7, 8 based on 3/4 and 7/8 grades
	for row in cg_df.iterrows():
		if row[1][3]:
			per_3_4_grade = row[1][3]
			row[1][2] = per_3_4_grade
			row[1][4] = per_3_4_grade
		elif row[1][8]:
			per_7_8_grade = row[1][8]
			row[1][7] = per_7_8_grade
	
	# drop cols for periods 3/4 and 7/8
	cg_df.drop(['03/04 Per', '07/08 Per', '09 Per'], axis=1, inplace=True)	
	
	# fill the NaNs with -1.0
	cg_df.fillna(-1.0, inplace=True)
	
	# # round the grades down to nearest ten
	cg_df['p1_r'] = cg_df.p1.apply(round_grade)
	cg_df['p2_r'] = cg_df.p2.apply(round_grade)
	cg_df['p3_r'] = cg_df.p3.apply(round_grade)
	cg_df['p4_r'] = cg_df.p4.apply(round_grade)
	cg_df['p5_r'] = cg_df.p5.apply(round_grade)
	cg_df['p6_r'] = cg_df.p6.apply(round_grade)
	cg_df['p7_r'] = cg_df.p7.apply(round_grade)
	cg_df['p8_r'] = cg_df.p8.apply(round_grade)
	
	# add the letter grade columns
	cg_df['p1_letter'] = cg_df.p1_r.apply(letter_grade)
	cg_df['p2_letter'] = cg_df.p2_r.apply(letter_grade)
	cg_df['p3_letter'] = cg_df.p3_r.apply(letter_grade)
	cg_df['p4_letter'] = cg_df.p4_r.apply(letter_grade)
	cg_df['p5_letter'] = cg_df.p5_r.apply(letter_grade)
	cg_df['p6_letter'] = cg_df.p6_r.apply(letter_grade)
	cg_df['p7_letter'] = cg_df.p7_r.apply(letter_grade)
	cg_df['p8_letter'] = cg_df.p8_r.apply(letter_grade)
	
	# add percentage signs
	cg_df['p1'] = cg_df.p1.apply(add_percentage)
	cg_df['p2'] = cg_df.p2.apply(add_percentage)
	cg_df['p3'] = cg_df.p3.apply(add_percentage)
	cg_df['p4'] = cg_df.p4.apply(add_percentage)
	cg_df['p5'] = cg_df.p5.apply(add_percentage)
	cg_df['p6'] = cg_df.p6.apply(add_percentage)
	cg_df['p7'] = cg_df.p7.apply(add_percentage)
	cg_df['p8'] = cg_df.p8.apply(add_percentage)
	
	# drop cols for periods 3/4 and 7/8
	cg_df.drop(['p1_r', 'p2_r', 'p3_r', 'p4_r', 'p5_r', 'p6_r', 'p7_r', 'p8_r'], axis=1, inplace=True)	
	
	# replace values
	cg_df.replace('-1.0%', '-', inplace=True)
	
	return cg_df


def round_grade(grade):
	"""
	This is a helper function that determines the rounded grade
	given a float of a student's grade in a particular class
	"""
	# only round grade if the input is a float
	if type(grade) != str:
		rounded_grade = np.floor(grade/10)
		return rounded_grade
	# otherwise return the original grade -> could be string "Lunch" or "Period"
	else:
		return grade


def letter_grade(grade):
	"""
	This is a helper function that determines the letter grade
	given a rounded float of a student's grade in a particular class.
	"""
	# define dictionary of grading scale
	# Check the fillna above was filled w/ -1.0
	d = {18.0: 'A', 17.0: 'A', 16.0: 'A', 15.0: 'A', 14.0: 'A', 13.0: 'A',
	12.0: 'A', 11.0: 'A', 10.0: 'A', 9.0: 'A', 8.0: 'B', 
	7.0: 'C', 6.0: 'D', 5.0: 'F', 4.0: 'F', 3.0: 'F', 2.0: 'F', 
	1.0: 'F', 0.0: 'F', -1.0: '-'}
	
	# get letter grade only if grade is not a string
	if type(grade) != str:
		# get the letter
		letter = d[grade]
		return letter
	else:
		return grade

def add_percentage(grade):
	"""
	This is a helper function that adds a percentage sign 
	to a float.
	"""
	if type(grade) == float:
		perc_grade = str(grade) + '%'
		return perc_grade
	else:
		return grade


def college_selectivity():
	"""
	This function assigns each student a college selectivity level
	based on their GPA and PSAT/SAT score.

	Input:
		- 
	Output:
		- 
	"""
    # unfinished
    d = {}

def import_student_emails(filename):
	"""
	This function takes csv file of STUDENT EMAILS and 
	returns a cleaned pandas dataframe w/ columns renamed.

	Input:
		- filename: a string name of the excel file
	Output:
		- email_df: a pandas dataframe of student emails
	"""
	email_df = pd.read_excel(filename, sheetname="All",
		usecols=[5,6])
	email_df.set_index('ID#', inplace=True)
	email_df = email_df.rename(index=int, columns={"CPS Email Address": "email",})
	email_df.index.names = ['ID']
	
	return email_df	

######### STEP 2 ##########
#   MERGE THE DATAFRAMES  #
#	INTO A SINGLE DATA-   #
#			FRAME         #
###########################

def master_dataframe(threshold):
	"""
	This function makes calls on each function that imports and cleans
	each csv file, concats the dataframes into a single master dataframe 
	while also creating new dataframes divided by grade level.

	Input:
		- threshold: integer of the max # of empty cells per student
					 we are willing to tolerate in the master dataframe
	Output:
		- master_dataframe: dataframe of all student data
		- nine: dataframe of all 9th grade student data
		- ten: dataframe of all 10th grade student data
		- eleven: dataframe of all 11th grade student data
		- twelve: dataframe of all 12th grade student data
	"""
	# import each of the dataframes
	gpa = import_gpa_data(GPA_DATA)
	rank = import_rank_data(CLASS_RANK_DATA)
	week = import_week_attn_data(WEEKLY_ATTN_DATA)
	year = import_year_attn_data(YTD_ATTN_DATA)
	swipe = import_swipe_data(SWIPE_DATA)
	sat = import_sat([SAT_9, SAT_10, SAT_11])
	email = import_student_emails(EMAIL_LIST)
	curr_grades = import_current_grades(CURRENT_GRADES)
	service = import_service_learning([SL_11, SL_12])
	master_dataframe = pd.concat([email,gpa,rank,sat,week,year,swipe,curr_grades, service],axis=1)
	
	# fill late_date and late_time columns NaNs
	master_dataframe[['late_date','late_time']] = master_dataframe[['late_date', 'late_time']].fillna(value='None!')
	
	# fill in service learning for 9th and 10th graders
	master_dataframe['service_hours'] = master_dataframe['service_hours'].fillna(value="See Note Below for Freshman & Sophomores")

	# add the date
	master_dataframe['start_date'] = START_DATE
	master_dataframe['end_date'] = END_DATE

	#master_dataframe[['orange_status']] = master_dataframe[['orange_status']].fillna(value="ARE NOT")
	master_dataframe[['composite_sat', 'erw_sat', 'math_sat']] = master_dataframe[['composite_sat', 
	'erw_sat', 'math_sat']].fillna(value="Scores coming in mid-May")
		
	# drop rows missing excessive amounts of data
	master_dataframe = master_dataframe.dropna(thresh=threshold)

	# obtain separate dataframes for each grade level
	# nine, ten, eleven, twelve = groupby_grade(master_dataframe)

	return master_dataframe


def groupby_grade(master_dataframe):
	"""
	This is a helper function that drops the student names of the 
	master dataframe and divides the master dataframe into
	four separate dataframes by grade.
	"""
	no_name_df = master_dataframe.drop(['last_name', 'first_name', 'email'], axis=1)
	grouped = list(no_name_df.groupby(['grade']))
	nine, ten, eleven, twelve = grouped[0][1], grouped[1][1], grouped[2][1], grouped[3][1]

	return nine, ten, eleven, twelve


def get_excel_spreadsheets(threshold):
	"""
	This function creates a single Excel file with multiple worksheets: a 
	master worksheet, and one for each grade level.

	Input:
		- threshold: integer of the max # of empty cells per student
					 we are willing to tolerate in the master dataframe
	"""
	master, nine, ten, eleven, twelve = master_dataframe(threshold)

	writer = pd.ExcelWriter('ALL_STUDENT_NUMBERS.xlsx')
	master.to_excel(writer,'ALL STUDENTS')
	nine.to_excel(writer,'GRADE_9')
	ten.to_excel(writer,'GRADE_10')
	eleven.to_excel(writer,'GRADE_11')
	twelve.to_excel(writer,'GRADE_12')
	writer.save()

def get_mail_merge(threshold):
	"""
	This function creates a single Excel file with all information for each student
	that has an email address

	Input:
		- threshold: integer of the max # of empty cells per student
					 we are willing to tolerate in the master dataframe

	RECOMMENDED THRESHOLD: 20
	"""	
	master = master_dataframe(threshold)
	master.dropna(subset=['email'],inplace=True)

	writer = pd.ExcelWriter('MAIL_MERGE.xlsx')
	master.to_excel(writer,'ALL STUDENTS')
	writer.save()

###### OTHER HELPER FUNCTIONS #########

def missing_emails(master_dataframe):
	"""
	This function obtains the first name, last name, id numbers of students
	who have missing emails from the email.csv file.

	Input:
		- master_dataframe: a pandas datafarme
	"""
	master, nine, ten, eleven, twelve = master_dataframe(threshold)
	missing_email = master_dataframe['email'].isnull()

	l = set()
	for id_num, value in missing_email.iteritems():
		if value == True:
			l.add(id_num)
	d = {}
	for i in master.iterrows():
		id_num = i[0]
		if id_num in l:
			last_name = i[1][1]
			first_name = i[1][2]
			name = (last_name, first_name)
			d[id_num] = name

	w = csv.writer(open("missing_email.csv", "w"))

	for key, val in d.items():
		w.writerow([key,val])


