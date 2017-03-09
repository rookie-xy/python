#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import os.path
import sys


TOPIC   = "--topic="
CHANNEL = "--channel="


def get_file_list(path):

	list = []

	for parent, dirnames, filenames in os.walk(path):
		for filename in filenames:
			list.append(os.path.join(parent, filename))

	return list


def stat_end(object):
	for i in object:
		print "\t%s\t" % i, object[i]


def get_topic(part):
	start = len(TOPIC)
	end = part.find(" ")

#	pos = part.find(TOPIC)
#	if pos != -1:
#		get_topic(part[pos:])

	return part[start : end].strip("\"")


def get_channel(part):
	start = len(CHANNEL)
	end = part.find(" ")

	return part[start : end].strip("\"")
	

def stat_process(line, hash):
	list = []

	topic = ""

	pos = line.find(TOPIC)
	if pos != -1:
		topic = get_topic(line[pos:])

	pos = line.find(CHANNEL)
	if pos != -1:
		channel = get_channel(line[pos:])

		if hash.has_key(topic):
			elem = hash[topic]
			elem.append(channel)
			hash[topic] = elem
			
			return

		list.append(channel)
		hash[topic] = list


def stat_start(filename):
	done = 0
	dict = {}

	fo = open(filename, "rb")
	if fo == None:
		print "open file error " + filename + "\n"

	while not done:
		line = fo.readline()

		if line != "":
			stat_process(line, dict)

		else:
			done = 1

	stat_end(dict)


if __name__ == '__main__':
	args = sys.argv
	path = "/data/service"

	start = 0
	end   = len(args)

	for i in range(start, end):
		if i == 0:
			continue
		path = args[i]

	filelist = get_file_list(path)
	if filelist == None:
		print "Not found file list"

	print "生产" + "\t" + "消息主题" + "\t" + "消费"
	for value in filelist:
		stat_start(value)
