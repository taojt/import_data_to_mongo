# !/usr/bin/python
# -*- coding:utf-8 -*-
__author__ = 'Tao Jiang'

from mongo_connect import *
import datetime

def test_xiugai():

	src_conn = connect("2013", "liepin_resume", host="192.168.3.224")
	dest_conn = connect("sanzhao_resume", "sanzhao_resume_1_v3", host="101.204.243.241")

	page = 0
	page_size = 1000
	temp = 0
	all_temp = 0
	start_time = datetime.datetime.now()
	while True:
		db_cursor = src_conn.find().skip(page_size*page).limit(page_size)

		page += 1
		temp = 0
		for d in db_cursor:
			resume = d
			if "_id" in resume:
				del resume["_id"]
			# 插入数据库
			dest_conn.update({"cv_id": resume["cv_id"], "source": resume["source"]}, resume, True, False)
			temp += 1
			all_temp += 1
		end_time = datetime.datetime.now()
		print u"完成导入 %s 条数据，一共耗时 %d 秒 ！" % (all_temp, (end_time - start_time).seconds)
		# 判断是否是已经读取完数据，因为是分页查询的，每页1000条数据，若不满1000条则是最后的数据
		if temp % page_size != 0 or temp == 0:
			print u"---------- 已经导入所有数据"
			break


if __name__ == '__main__':
    test_xiugai()