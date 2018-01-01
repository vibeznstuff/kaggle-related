import pandas as pd, sys, time, enchant, threading
from collections import Counter

### Global Vars ###
toxic_types = []
#toxic_types = ['toxic','severe_toxic','obscene','threat','insult','identity_hate']
timer = time.time() #Initial Time snapshot for tracking runtime
new_timer = timer	#End Time snapshot for tracking runtime
# Instantiate Spell checker dictionary
spell_checker = enchant.DictWithPWL("en_US","../additional_enchant_terms.txt")
typo_catches = {} #Record all suspected typos
record_counts = {} #Track record count by toxic type

#Threads for parallel processing
class MyThread(threading.Thread):
	def run(self):
		print("This is thread " + str(theVar) + " speaking.")
		print("Begin: " + str(begin) + ", End: " + str(end))
		df = pd.read_csv(r"C:\Users\Richard\Documents\open_data\my_data\tmp\fake_credit_scores2.csv")
		my_df = df.truncate(before=begin,after=end)
		sums.append(my_df.agg({'credit_score':['sum']}).iat[0,0])
		print(my_df.agg({'credit_score':['sum']}).iat[0,0])
		print("Hello and goodbye!")
		theVar +=1

#Evaluate if a typo exists and correct if possible
#	e.g. "exelent" -> "excellent"
def recommend_typo_fix(text):
	if spell_checker.check(text.upper()):
		return text
	else:
		possible_matches = [x.lower() for x in spell_checker.suggest(text) if x.lower()[0] == text.lower()[0]]
		if len(possible_matches) == 1:
			typo_catches[text]=possible_matches[0]
			return possible_matches[0]
		elif len(possible_matches) > 1:
			typo_catches[text]="*"+possible_matches[0]
			return text
		else:
			return text

#De-duplicate characters in text where characters
#have been duplicated for emphasis
#	e.g. "Heeeeeey" -> "Hey"
def remove_emphasis_dupes(text):
	#Identify potential dupe offenders
	new_text = text
	if not spell_checker.check(new_text.upper()):
		cts = Counter(text)
		risk_chars = [(cts[x],x) for x in cts if cts[x] > 1]
		risk_chars.sort(reverse=True)
		for ct,char in risk_chars:
			if not spell_checker.check(new_text.upper()):
				fst = new_text.find(char)
				lst = new_text.rfind(char)
				flag = (new_text[int((fst+lst)/2)] == char)
				if flag:
					try_word = new_text[0:fst+1] + new_text[lst:]
					if spell_checker.check(try_word.upper()):
						new_text= try_word
						break
					else:
						new_text = new_text[0:fst] + new_text[lst:]
			else:
				break
	return new_text

#Print out elapsed time for specific processes
def log_snapshot(process_name):
	global timer, new_timer
	new_timer = time.time()
	elapsed_seconds = (new_timer - timer)
	timer=new_timer
	if elapsed_seconds >= 3600:
		print(process_name + " has completed after " + str(round(elapsed_seconds/3600.0,2)) + " hours.")
		return elapsed_seconds
	elif elapsed_seconds >= 60:
		print(process_name + " has completed after " + str(round(elapsed_seconds/60.0,2)) + " minutes.")
		return elapsed_seconds
	elif elapsed_seconds < 60:
		if elapsed_seconds >= 1:
			print(process_name + " has completed after " + str(round(elapsed_seconds,0)) + " seconds.")
		return elapsed_seconds
	else:
		return -1

#Clean up the comments from the raw data
#to allow for higher quality processing
def clean_comments(text):
	global spell_checker
	unwanted_chars = ['!',';','~',':','.','[',']',',','\"', \
					'?','&','%','$','@','{','}','\\','/','_', \
					'-','<','>','(',')','*','^','#','|','\'','=']
	new_text = text
	
	#Remove unwanted characters from comment text
	for char in unwanted_chars:
		new_text = new_text.replace(char,"")
	
	log_snapshot("Taken out unwanted chars for " + text)
		
	#Get distinct terms within a comment
	term_list = list(set(new_text.split()))
	
	log_snapshot("Obtained distinct terms for" + text)
	
	#Get rid of duplicate characters used for exaggeration
	term_list = [remove_emphasis_dupes(x) for x in term_list]
	
	log_snapshot("Remove emphasis dupes for " + text)
	
	#Correct typos in comment
	for term in term_list:
		recommend_typo_fix(term)
	
	log_snapshot("Correct typos in comment")
	
	#Remove terms which are unreasonably long as they are likely junk
	term_list = [x for x in term_list if len(x) < 50]
	
	log_snapshot("Remove terms which are suspiciously long for" + text)
	
	return term_list
	
#Create the term frequencies for each type of toxic type,
#including the comments which are not toxic (or normal,'base')
def create_terms_by_type(file_path):
	df = pd.read_csv(file_path)
	print("Dataframe size before cleaning: " + str(len(df)))
	# <Enter data cleaning code here>
	print("Dataframe size after cleaning: " + str(len(df)))
	
	#Create separate subsets for training against specific
	#toxic types
	terms_by_type = {}
	for type in toxic_types:
		terms_by_type[type]=[]
		tmp_df = df.loc[df[type] == 1]
		record_counts[type] = len(tmp_df)
		print(str(record_counts[type]) + " records for '" + type + "' toxic type.")
		for index, row in tmp_df.iterrows():
			comment = clean_comments(row['comment_text'].lower())
			terms_by_type[type] = terms_by_type[type] + comment
		#tmp_df.to_csv("../output/"+type+"_train.csv")
		log_snapshot("Creating terms for " + type)
	
	#Create subset of data for 'base' or standard comments
	df_base = df.loc[(df['toxic'] == 0) & (df['toxic'] == 0) & \
			(df['severe_toxic'] == 0) & (df['obscene'] == 0) & (df['threat'] == 0) & \
			(df['insult'] == 0) & (df['identity_hate'] == 0)]
	record_counts['base'] = len(df_base)
	print(str(record_counts['base']) + " records for 'base' toxic type.")
	terms_by_type['base']=[]
	for index, row in df_base.iterrows():
		comment = clean_comments(row['comment_text'].lower())
		terms_by_type['base'] = terms_by_type['base'] + comment
	#df_base.to_csv("../output/base_train.csv")
	log_snapshot("Creating terms for base")
	
	return terms_by_type

#Produce the top terms for each toxic type
def reveal_top_terms_by_type(terms_dict):
	term_freqs = {}
	for type in toxic_types:
		log_snapshot("Creating top terms for type: " + type)
		counts = Counter(terms_dict[type])
		term_freqs[type] = [(counts[x],x) for x in counts if counts[x] > 1]
		term_freqs[type].sort(reverse=True)
	return term_freqs

def score_term_significance(term_freqs, base_terms):
	significant_terms = {}
	base_counts = Counter(base_terms)
	for type in toxic_types:
		log_snapshot("Scoring term significance for type: " + type)
		significant_terms[type] = []
		for freq, term in term_freqs[type]:
			base_freq = base_counts[term]
			tmp_freq_norm = float(freq)/float(record_counts[type])
			base_freq_norm = float(base_freq)/float(record_counts['base'])
			freq_ratio = round(tmp_freq_norm/base_freq_norm,2)
			if freq_ratio > 2:
				significant_terms[type] = significant_terms[type] + [(term,freq_ratio)]
		
#Run end-to-end data preparation process
def main():
	terms_by_type = create_terms_by_type(r"C:\Users\Richard\Documents\open_data\kaggle\wiki_toxic_comments\train.csv")
	term_freqs = reveal_top_terms_by_type(terms_by_type)
	significant_terms = score_term_significance(term_freqs,terms_by_type['base'])
	print(significant_terms)

if __name__ == "__main__":
	main()