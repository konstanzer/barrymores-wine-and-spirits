import pandas as pd
from os import path
import warnings
import argparse

warnings.filterwarnings("ignore")
dir_path = path.dirname(path.realpath(__file__))


def load_data(input_file):
	return pd.read_excel(path.join(dir_path, input_file), header=None)


def change_item_names(df):
	'''
	Replace the plus signs in items names.
	'''
	df.iloc[12:27].replace('NORTHERN \+', 'NORTHERN PLUS', regex=True, inplace=True)
	df.iloc[626].replace('YR \+', 'YR PLUS', regex=True, inplace=True)
	df.iloc[1528].replace('\+ CHASE', 'PLUS CHASE', regex=True, inplace=True)
	df.iloc[2015:2035].replace('90\+', '90 PLUS', regex=True, inplace=True)
	df.iloc[2635:2642].replace('90\+', '90 PLUS ', regex=True, inplace=True)
	df.iloc[2691].replace('\+CHASE', ' PLUS CHASE', regex=True, inplace=True)

	return df


def select_columns(df):
	'''
	Split the columns and keep only the relevant ones.
	'''
	df = df[0].str.split('+', 0, expand=True)
	item = df[0]
	df = df[[7, 10, 36]]
	df = df.rename(columns={7:'avg_cost', 10:'price', 36:'quantity'})
	#Create empty fields then rearrange order.
	df[['item_no', 'item', 'category', 'subcategory', 'description']] = None
	df = df[['item_no','item','category','subcategory','avg_cost',
				'price','quantity','description']]

	return item, df


def numeric_fields(df):
	'''
	Extract substrings for price, cost, and quantity fields.
	'''
	df['avg_cost'] = df['avg_cost'].str[3:6] + '.' + df['avg_cost'].str[6:8]
	df['avg_cost'] = df['avg_cost'].astype(float)

	df['price'] = df['price'].str[21:24] + '.' +  df['price'].str[24:26]
	df['price'] = df['price'].astype(float)
	
	df[['quantity', 'pos']] = df['quantity'].str.strip().str.split('-', 0, expand=True)
	df['quantity'] = df['quantity'].str[5:9].astype(int)
	
	#The pos field indicates whether quantity is positive.
	df['pos'] = df['pos'].isnull()
	df['pos'][df['pos']==False] = df['pos'] - 1
	df['quantity'] = df['quantity']*df['pos']
	df = df.drop('pos', axis=1)
	
	return df


def divvy_field(item, df):
	'''
	Divides the big item field into item_no, item, category,
	subcategory, and description.
	'''
	item.replace('INN000','', regex=True, inplace=True)
	categories = ('LIQ', 'WIN', 'MER', 'WHI')

	for ix, row in enumerate(item):

	  label, make_label = '', True
	  substr = row.split()

	  for ix2, s in enumerate(substr):

	    if 'B0' in s and len(s) > 30:

	      make_label = False
	      suffix = -3 if s.endswith(categories) else None

	      if suffix:
	      	df.category[ix] = s[suffix:]
	      	df.subcategory[ix] = substr[ix2+1][:5]
	      else:
	        df.category[ix] = ''
	        df.subcategory[ix] = ''

	      sku_raw = [i for i in s[:suffix].split('B') if len(i) > 10]
	      df.item_no[ix] = sku_raw[1]
	      #Adds the word attached to SKU to label field.
	      df.item[ix] = label + sku_raw[0].replace(sku_raw[1], '')

	    if make_label:
	      label += s + ' '
	    else:
	      if 'ACT0' in s:
	        df.description[ix] = ' '.join([label.split()[0].capitalize()] + substr[ix2+1:])
	        break

	    #There's a different format at tail-end.
	    if ix > 3043:
	    	df.item[ix] = ' '.join(label.split()[:4])
	    	if s.endswith(categories):
	    		df.category[ix] = s[-3:]
	    		df.subcategory[ix] = substr[ix2+1][:5]
	    		df.description[ix] = ' '.join(substr[ix2+5:])
	    		break

	return df


def clean_data(input_file, output_file):
	'''
	Combine all functions to output a clean dataframe.
	'''
	df = load_data(input_file)
	print('Data loaded.')
	df = change_item_names(df)
	item, df = select_columns(df)
	print('Calculating...')
	df = numeric_fields(df)
	df = divvy_field(item, df)
	df.to_csv(path.join(dir_path, output_file))
	print(f'Success. File saved as {output_file}.')

	return df


if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("input", help="The name of the .xlsx file.")
	args = parser.parse_args()
	df = clean_data(args.input, 'clean_spirits.csv')