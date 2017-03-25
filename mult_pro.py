# coding:utf-8
# my_dir='/home/shouldchan/文档/multi_test/'

# import multiprocessing
# import time


# def read_City_data(phidList):
#     with open(my_dir + 'userVisits-Toro.txt', 'r') as frpho:
#         lines = frpho.readlines()
#         # phidList = []
#         for line in lines:
#             tempData = line.strip().split('\t')
#             photoid = tempData[0]
#             phidList.append(photoid)
#     return phidList



# result_list = []
# SHOW = 0

# def read_dataset(start, end):
#     fopen = open(my_dir + 'temp_data.txt', 'r')
#     lines = fopen.readlines()[start:end]
#     for eachline in lines:
#         global SHOW
#         tempData = eachline.strip().split('\t')
#         pho_id, urlData, isPhoto = tempData[1], tempData[16], tempData[24]
#         if pho_id in phidList and isPhoto == '0' and urlData != '':
#             result_list.append([pho_id,urlData])
#             SHOW += 1
#             print SHOW
#             # with open(result_dir + 'Toro_photoID_url.txt', 'wb') as fwrite:
#             #     fwrite.write(pho_id + '\t' + urlData + '\n')
#     return result_list

# # read_dataset(0, 23712)
# # print 'write photoID_url elapsed:', time.time() - t1


# if __name__ == '__main__':
#     t0 = time.time()
#     phidList = []
#     read_City_data(phidList)
#     print phidList
#     print 'read_City_data elapsed:', time.time() - t0

#     t1 = time.time()
#     read_dataset(0,23712)
#     print 'read_dataset elapsed:', time.time() - t1

#     t2 = time.time()
#     pool = multiprocessing.Pool(processes=4)

#     s = 0
#     e = 0
#     for i in range(0, 4):
#         s = e
#         e = s + 5928
#         res = pool.apply_async(read_dataset, (s, e))
#     pool.close()
#     pool.join()
#     print 'muliwrite photoID_url elapsed:', time.time() - t2
#     print result_list

#     # write in txt file
#     with open(my_dir + 'Toro_photoID_url.txt', 'wb') as fwrite:
#         for [x,y] in result_list:
#             fwrite.write(str(x)+"\t"+str(y)+"\n")
#     print 'write in ok...'



# my_dir='/home/shouldchan/文档/multi_test/'
result_dir='./result/'
data_dir='./data/'

import multiprocessing
import time

t1 = time.time()
phidList = []

def read_City_data():
    with open(result_dir + 'userVisits-Toro.txt', 'r') as frpho:
        lines = frpho.readlines()
        # phidList = []
        for line in lines:
            tempData = line.strip().split('\t')
            photoid = tempData[0]
            phidList.append(photoid)
    # print phidList

read_City_data()
print 'read City_data elapsed: ',time.time() - t1

SHOW = 0

def read_dataset(start, end):
    fopen = open(data_dir + 'yfcc100m_dataset.txt', 'r')
    result_list = []
    lines = fopen.readlines()[start:end]
    for eachline in lines:
        global SHOW
        tempData = eachline.strip().split('\t')
        pho_id, urlData, isPhoto = tempData[1], tempData[16], tempData[24]
        if pho_id in phidList and isPhoto == '0' and urlData != '':
            result_list.append([pho_id,urlData])
            SHOW += 1
            print SHOW
            # print pho_id
            # print urlData
    return result_list


t2 = time.time()
pool = multiprocessing.Pool(processes=4)

result = []
s = 0
e = 0
for i in range(0, 4):
    s = e
    e = s + 5928
    res = pool.apply_async(read_dataset, (s, e))
    result+=res.get()
pool.close()
pool.join()
print 'muliwrite photoID_url elapsed: ', time.time() - t2
# print result

# write in txt file
with open(result_dir + 'Toro_photoID_url.txt', 'wb') as fwrite:
    for [x,y] in result:
        fwrite.write(str(x)+"\t"+str(y)+"\n")
print 'write in ok...'


    