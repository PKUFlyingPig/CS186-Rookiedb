import argparse
import json
import os
import re
import shutil
import tempfile
import subprocess

PROJ_LIST = ['proj0', 'proj2', 'proj3', 'proj4', 'proj5']

def files_to_copy(assignment):
    files = {
        'proj0': ['src/main/java/edu/berkeley/cs186/database/databox/StringDataBox.java'],
        'proj2': [
            'src/main/java/edu/berkeley/cs186/database/index/BPlusTree.java',
            'src/main/java/edu/berkeley/cs186/database/index/BPlusNode.java',
            'src/main/java/edu/berkeley/cs186/database/index/InnerNode.java',
            'src/main/java/edu/berkeley/cs186/database/index/LeafNode.java',
            'src/main/java/edu/berkeley/cs186/database/index/BPlusNode.java'
        ],
        'proj3': [
            'src/main/java/edu/berkeley/cs186/database/query/join/BNLJOperator.java',
            'src/main/java/edu/berkeley/cs186/database/query/join/SortMergeOperator.java',
            'src/main/java/edu/berkeley/cs186/database/query/join/GHJOperator.java',
            'src/main/java/edu/berkeley/cs186/database/query/SortOperator.java',
            'src/main/java/edu/berkeley/cs186/database/query/QueryPlan.java',
        ],
        'proj4': [
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockType.java',
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockManager.java',
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockContext.java',
            'src/main/java/edu/berkeley/cs186/database/concurrency/LockUtil.java',
            'src/main/java/edu/berkeley/cs186/database/table/Table.java',
            'src/main/java/edu/berkeley/cs186/database/table/PageDirectory.java',
            'src/main/java/edu/berkeley/cs186/database/memory/Page.java',
            'src/main/java/edu/berkeley/cs186/database/Database.java'
        ],
        'proj5': [
            'src/main/java/edu/berkeley/cs186/database/recovery/ARIESRecoveryManager.java',
        ],
    }
    return files[assignment]

def get_path(proj_file):
    index = proj_file.rfind('/')
    if index == -1:
        return ''
    return proj_file[:index]

def get_dirs(proj_files):
    dirs = set()
    for proj in proj_files:
        dirs.add(get_path(proj))
    return dirs

def create_proj_dirs(tempdir, assignment, dirs):
    for d in dirs:
        try:
            tmp_proj_path = tempdir + '/'  + d
            if not os.path.isdir(tmp_proj_path):
                os.makedirs(tmp_proj_path)
        except OSError:
            print('Error: Creating directory %s failed' % tmp_proj_path)
            exit()
    return tempdir + '/'

def copy_file(filename, proj_path, tmp_proj_path):
    student_file_path = proj_path + '/' + filename
    tmp_student_file_path = tmp_proj_path + '/' + get_path(filename)
    if not os.path.isfile(student_file_path):
        print('Error: could not find file at %s' % student_file_path)
        exit()
    shutil.copy(student_file_path, tmp_student_file_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='proj submission script')
    parser.add_argument('--assignment', help='assignment number', choices=PROJ_LIST)
    args = parser.parse_args()

    if not args.assignment:
        args.assignment = input('Please enter the assignment number (one of {}): '.format(str(PROJ_LIST)))
        if args.assignment not in PROJ_LIST:
            print('Error: please make sure you entered a valid assignment number')
            exit()

    with tempfile.TemporaryDirectory() as tempdir:
        proj_files = files_to_copy(args.assignment)
        dirs = get_dirs(proj_files)
        print(dirs)
        tmp_proj_path = create_proj_dirs(tempdir, args.assignment, dirs)
        for filename in proj_files:
            copy_file(filename, os.getcwd(), tmp_proj_path)

        # Create zip file
        proj_zip_path = os.getcwd() + '/' + args.assignment + '.zip'
        shutil.make_archive(args.assignment, 'zip', os.path.join(tempdir, 'src'))

        print('Created ' + args.assignment + '.zip')
