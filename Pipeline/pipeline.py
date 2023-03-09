import luigi
import subprocess
import time

class StartDocker(luigi.Task):

    def run(self):
        subprocess.run(['docker-compose', '-f', 'C:\\Users\\ehub9\\Desktop\\DTC_Capstone_Project\\Pipeline\\docker-compose.yaml', 'up', '-d', '--build'], check=True)
        print('Started Docker')
        time.sleep(5)

    def output(self):
        return luigi.LocalTarget("docker_started.txt")

class IngestData(luigi.Task):

    def requires(self):
        return StartDocker()

    def run(self):
        subprocess.run(['python', 'C:\\Users\\ehub9\\Desktop\\DTC_Capstone_Project\\Pipeline\\ingest_data.py'], check=True)
        print('Ingested data')
        time.sleep(5)

    def output(self):
        return luigi.LocalTarget("data_ingested.txt")

class ETL1(luigi.Task):

    def requires(self):
        return IngestData()

    def run(self):
        subprocess.run(['python', 'C:\\Users\\ehub9\\Desktop\\DTC_Capstone_Project\\Pipeline\\etl1.py'], check=True)
        print('ETL1 completed')
        time.sleep(5)

    def output(self):
        return luigi.LocalTarget("etl1_complete.txt")

class ETL2(luigi.Task):

    def requires(self):
        return ETL1()

    def run(self):
        subprocess.run(['python', 'C:\\Users\\ehub9\\Desktop\\DTC_Capstone_Project\\Pipeline\\etl2.py'], check=True)
        print('ETL2 completed')
        time.sleep(5)

    def output(self):
        return luigi.LocalTarget("etl2_complete.txt")

class RunPipeline(luigi.Task):

    def requires(self):
        return ETL2()

    def run(self):
        print('Pipeline completed')
        with self.output().open('w') as f:
            f.write('Pipeline complete')

    def output(self):
        return luigi.LocalTarget("pipeline_complete.txt")
      
if __name__ == '__main__':
    luigi.run(['RunPipeline', '--local-scheduler'])
