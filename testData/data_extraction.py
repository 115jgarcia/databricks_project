# Databricks notebook source
class extract():
    def __init__(self):
        self.csv_files = []

    def get_file_paths(self):
        files = dbutils.fs.ls('gs://bankdatajg/generator')
        for i in files:
            l1_files = dbutils.fs.ls(i[0])
            for j in l1_files:
                l2_files = j[0]
                if '.csv' in l2_files:
                    self.csv_files.append(l2_files)

    def land_files_to_raw(self):
        self.get_file_paths()
        for i in self.csv_files:
            from_path = i
            split_to_path = from_path.split('/')
            split_archive = from_path.split('/')
            
            # create archive path
            split_archive[3] = 'archive'
            from_archive_path = '/'.join(split_to_path[:-1])
            archive_path = '/'.join(split_archive[:-1])
            
            # create new path
            split_to_path[3] = 'raw'
            nw_dir = split_to_path[4].split('_')
            split_to_path[4] = f"{nw_dir[-1]}"
            split_to_path[5] = f"{nw_dir[-1]}_{int(float(nw_dir[0]))}.csv"

            to_path = '/'.join(split_to_path)
            
            # cp from_path to to_path
            print(f"Moving: {from_path}")
            dbutils.fs.cp(from_path, to_path)
            
            # archive copied file
            dbutils.fs.mv(from_archive_path, archive_path, True)
