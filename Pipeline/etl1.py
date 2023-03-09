import luigi, os, time
import pandas as pd
from google.cloud import translate_v2 as translate


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "#YOUR CREDENTIALS"



class CleanFrenchData(luigi.Task):    
   
    def output(self):
        output_path = 'C:/Users/ehub9/Desktop/DTC_Capstone_Project/Pipeline/cleaned_french.csv'
        return luigi.LocalTarget(output_path)
    
    def run(self):
        input_path = 'C:/Users/ehub9/Desktop/DTC_Capstone_Project/Pipeline/output.csv'
        print('Reading input file...')
        df = pd.read_csv(input_path, delimiter=';', encoding='latin-1')
        
        print('Replacing French accented characters...')
        df.replace({    'a': 'a',    'à': 'a',    'â': 'a',    'ä': 'a',    'ç': 'c',    'e': 'e',    'é': 'e',    'è': 'e', 
       'ê': 'e',    'ë': 'e',    'i': 'i',    'î': 'i',    'ï': 'i',    'o': 'o',    'ô': 'o',    'ö': 'o',    'u': 'u',    'ù': 'u',
        'û': 'u',    'ü': 'u',    'ç': 'c',    'Ç': 'C',    'À': 'A',    'Â': 'A',    'Ä': 'A',    'È': 'E',    'É': 'E', 
       'Ê': 'E',    'Ë': 'E',    'Ï': 'I',    'Î': 'I',    'Ô': 'O',    'Ö': 'O',    'Ù': 'U',    'Û': 'U',    'Ü': 'U'}, regex=True, inplace=True)
        
        print('Writing cleaned data to output file...')
        df.to_csv(self.output().path, sep=';', index=False, encoding='utf-8')
        time.sleep(5)


class FillNAN(luigi.Task):

    def requires(self):
        return CleanFrenchData()
    
    def output(self):
        return luigi.LocalTarget(os.path.join('C:/Users/ehub9/Desktop/DTC_Capstone_Project/Pipeline/cleaned_french.csv'))

    def run(self):
        print('Filling NaN values...')
        df = pd.read_csv("C:/Users/ehub9/Desktop/DTC_Capstone_Project/Pipeline/cleaned_french.csv", sep=";", encoding="utf-8")
        df.fillna(value=0, inplace=True)  
        df.to_csv(self.output().path, sep=";", index=False, encoding="utf-8")
        print('NaN values filled.')
        time.sleep(5)


class Translate(luigi.Task):

    local_path = "translated_data.csv"
    chunksize = 33

    def requires(self):
        return FillNAN()

    def output(self):
        return luigi.LocalTarget(self.local_path)

    def run(self):
        print(f"Translating file {self.input().path}...")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "#YOUR CREDENTIALS"
        client = translate.Client()
        chunks = pd.read_csv(self.input().path, chunksize=self.chunksize, sep=";")
        df_list = []
        chunk_counter = 0
        start_time = time.time()
        for chunk in chunks:
            for col in chunk.columns:
                chunk[col] = chunk[col].apply(lambda x: client.translate(f'"{x}"', target_language='en')['translatedText'])
            df_list.append(chunk)
            chunk_counter += 1
            print(f"Translated {chunk_counter} chunks.")
        df = pd.concat(df_list)
        df.to_csv(f"C:/Users/ehub9/Desktop/DTC_Capstone_Project/Pipeline/{self.local_path}", index=False)
        elapsed_time = time.time() - start_time
        print(f"File translated and saved to {self.local_path}.")
        print(f"Elapsed time: {elapsed_time:.2f} seconds")
        time.sleep(5)


class CSVtoParquet(luigi.Task):
    local_path = "C:/Users/ehub9/Desktop/DTC_Capstone_Project/Pipeline/translated_data.csv"
    output_path = "C:/Users/ehub9/Desktop/DTC_Capstone_Project/Pipeline/translated_data.parquet"
    
    def requires(self):
        return Translate()
    
    def output(self):
        return luigi.LocalTarget(self.output_path)
    
    def run(self):
        print(f"Reading CSV file from {self.local_path}...")
        df = pd.read_csv(self.local_path)
        print(f"CSV file loaded into DataFrame with shape {df.shape}.")
        print(f"Converting DataFrame to parquet file and saving to {self.output_path}...")
        df.to_parquet(self.output_path, index=False)
        print(f"Parquet file saved to {self.output_path}.")    
        time.sleep(5)

if __name__ == '__main__':
    luigi.run(['CSVtoParquet', '--local-scheduler'])
