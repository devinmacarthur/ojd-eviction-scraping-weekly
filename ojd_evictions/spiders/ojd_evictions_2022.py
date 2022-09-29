# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from scrapy.http import FormRequest
import json
import logging
import string
import requests
from lxml import html
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.loader.processors import Join
from scrapy.utils.python import to_native_str
from urllib.parse import urljoin
from twisted.internet import reactor, defer
import re
from ojd_evictions.items import CaseOverviewItem, CasePartyItem, LawyerItem, JudgmentItem, FileItem, EventItem
import calendar
from time import sleep
from datetime import date, datetime, timedelta
import pandas as pd
from scrapy.crawler import CrawlerProcess

today = date.today()


class OJDEvictions(scrapy.Spider):
	name = 'ojd-evictions-2022'

	allowed_domains = ['publicaccess.courts.oregon.gov']

	logging.basicConfig(filename="logfile.log", level = logging.DEBUG)
	logger = logging.getLogger(__name__)

	url_login = "https://publicaccess.courts.oregon.gov/PublicAccessLogin/login.aspx"
	url_case_search = "https://publicaccess.courts.oregon.gov/PublicAccessLogin/Search.aspx?ID=200"
	url_case_det_base = "https://publicaccess.courts.oregon.gov/PublicAccessLogin/"

	login_cookie = {
		"BIGipServerOdy_Prod~ODY_Prod_PA_pool_443": "3002871306.47873.0000"
	}

	login_params = {
		'UserName': 'PSHMUL01',
		'Password': 'Zabat2019!',
		'ValidateUser': '1',
		'dbKeyAuth': 'JusticePA',
		'SignOn': 'Sign On'
	}

	location_dict = {
		'Multnomah': '104100,104210,104215,104220,104225,104310,104320,104330,104410,104420,104430,104440',
		'Washington': '120100',
		'Clackamas': '105100',
		'Baker': '108100',
		'Benton': '121100',
		'Clatsop': '118100',
		'Columbia': '119100',
		'Coos': '115100',
		'Crook': '122100',
		'Curry': '115200',
		'Deschutes': '111100',
		'Douglas': '116100',
		'Gilliam': '107100',
		'Grant': '124100',
		'Harney': '124200',
		'Hood River': '107200',
		'Jackson': '101100',
		'Jefferson': '122200',
		'Josephine': '114100',
		'Klamath': '113100',
		'Lake': '126100',
		'Lane': '102100',
		'Lincoln': '117100',
		'Linn': '123100',
		'Malheur': '109100',
		'Marion': '103100',
		'Morrow': '106100',
		'Polk': '112100',
		'Sherman': '107300',
		'Tillamook': '127100',
		'Umatilla': '106200',
		'Umatilla-Hermiston': '106210',
		'Union': '110100',
		'Wasco': '107400',
		'Wallowa': '110200',
		'Wheeler': '107500',
		'Yamhill': '125100'
	}

	case_search_form = {
		'__EVENTTARGET':'',
		'__EVENTARGUMENT':'',
		'SearchBy': '0',
		'ExactName': 'on',
		'CaseSearchMode': 'CaseNumber',
		'CaseSearchValue': '22LT*', # change this for a different case search
		'CitationSearchValue': '',
		'CourtCaseSearchValue': '',
		'PartySearchMode': 'Name',
		'AttorneySearchMode': 'Name',
		'LastName': '',
		'FirstName': '',
		'cboState': 'AA',
		'MiddleName': '',
		'DateOfBirth': '',
		'DriverLicNum': '',
		'CaseStatusType': '0',
		'DateFiledOnAfter': '', # change this to get different filing dates
		'DateFiledOnBefore': '', # change this to get different filing dates
		'chkCriminal': 'on',
		'chkFamily': 'on',
		'chkCivil': 'on',
		'chkProbate': 'on',
		'chkDtRangeCriminal': 'on',
		'chkDtRangeFamily': 'on',
		'chkDtRangeCivil': 'on',
		'chkDtRangeProbate': 'on',
		'chkCriminalMagist': 'on',
		'chkFamilyMagist': 'on',
		'chkCivilMagist': 'on',
		'chkProbateMagist': 'on',
		'DateSettingOnAfter': '',
		'DateSettingOnBefore': '',
		'SortBy': 'fileddate',
		'SearchSubmit': 'Search',
		'SearchType': 'CASE',
		'SearchMode': 'CASENUMBER',
		'NameTypeKy': '',
		'BaseConnKy': '',
		'StatusType': 'true',
		'ShowInactive': '',
		'AllStatusTypes': 'true',
		'CaseCategories': '',
		'RequireFirstName': '',
		'CaseTypeIDs': '',
		'HearingTypeIDs': '',
		'SearchParams': 'SearchBy~~Search By:~~Case~~Case||chkExactName~~Exact Name:~~on~~on||CaseNumberOption~~Case Search Mode:~~CaseNumber~~Number||CaseSearchValue~~Case Number:~~19LT*~~19LT*||AllOption~~All~~0~~All||selectSortBy~~Sort By:~~Filed Date~~Filed Date'
	}

	month_list = range(1, 13)
	DateRange = pd.date_range(start='01/01/2022', end=datetime.now(), freq='15D')
	# DateRange = pd.date_range(start='01/01/2015', end='12/30/2015', freq='15D')

	sleep_delay = 0.1

	def create_cookie_jar_from_resp(self, resp):
		cookie_jar = self.login_cookie
		for cookie in resp.cookies:
			cookie_jar[cookie.name] = cookie.value

		return cookie_jar

	# entry point into crawling
	def start_requests(self):
		# store the login information as form data
		login_formdata = self.login_params

		yield FormRequest(self.url_login, formdata = login_formdata, callback=self.parse_login,  meta={'dont_redirect': True, 'handle_httpstatus_list': [302]})

	# callback once logged in
	def parse_login(self, response):
		cookie_jar = self.login_cookie
		
		for cookie_item in response.headers.getlist('Set-Cookie'):
			cookie = cookie_item.decode("utf-8").split(';')[0]
			cookie_key = cookie.split('=')[0]
			cookie_val = cookie.split('=')[1]
			cookie_jar[cookie_key] = cookie_val

		# initiate searches for each county
		for loc, val in self.location_dict.items():	
			search_formdata = {
				'NodeID': val,
				'NodeDesc': loc
			}

			try:
				sleep(self.sleep_delay)
				resp = requests.post(url = self.url_case_search, data = search_formdata, cookies = cookie_jar)
				
				for item in self.parse_select_place(resp, val, loc):
					yield item
			except:
				pass

		# send POST request with the login info
	# callback once location selected		
	def parse_select_place(self, resp, node_id, location):
		cookie_jar = self.create_cookie_jar_from_resp(resp)

		tree = html.fromstring(resp.content)

		# grab all of the hidden input fields on the landing page (necessary to communicate state to the server)
		hidden_vals = tree.xpath("//input[@type='hidden']/@value")
		hidden_keys = tree.xpath("//input[@type='hidden']/@name")

		# build POST body data
		search_formdata = self.case_search_form

		search_formdata['NodeID'] = node_id
		search_formdata['NodeDesc'] = location

		for i in self.DateRange:

			starting_date = i.strftime("%m/%d/%Y")
			ending_date = (i.date() + timedelta(14)).strftime("%m/%d/%Y")
			
			# starting_date = "{month}/{day}/2022".format(month = str(month).zfill(2), day = '01')
			# ending_date = "{month}/{day}/2022".format(month = str(month).zfill(2), day = str(calendar.monthrange(2022,month)[1]).zfill(2))

			search_formdata['DateFiledOnAfter'] = starting_date
			search_formdata['DateFiledOnBefore'] = ending_date

			# add in the three non-empty fields to transfer state to the server (__VIEWSTATE, __VIEWSTATEGENERATOR, and __EVENTVALIDATION)
			for key, val in zip(hidden_keys, hidden_vals):
				if val == '':
					continue

				search_formdata[key] = val

			try:
				sleep(self.sleep_delay)
				resp = requests.post(url = self.url_case_search, data = search_formdata, cookies = cookie_jar)
				
				for item in self.parse_search_results(resp, location):
					yield item
			except:
				pass

	# callback for case search
	def parse_search_results(self, resp, location):
		# load in the cookies for the next request
		cookie_jar = self.create_cookie_jar_from_resp(resp)

		tree = html.fromstring(resp.content)

		select = Selector(text=resp.content.decode("utf-8"))

		# iterate over each row in the table
		for index, case in enumerate(select.xpath("//table[4]/tr[boolean(./td[1]/a)]")):
			print("Index: " + str(index))
			loader = ItemLoader(CaseOverviewItem(), case)

			loader.default_output_processor = Join()

			# load in the data for each case
			loader.add_xpath('url', ".//td/a/@href")
			loader.add_xpath('case_code', ".//td/a/text()")
			loader.add_xpath('style', ".//td[2]/text()")
			loader.add_xpath('date', ".//td[3]/div[1]/text()")
			loader.add_xpath('case_type', ".//td[4]/div[1]/text()", re="Landlord/Tenant - (.*)$")
			loader.add_xpath('status', ".//td[4]/div[2]/text()")
			loader.add_value('location', location)

			case_item = loader.load_item()
			yield case_item

			try:
				sleep(self.sleep_delay)
				resp = requests.post(url= self.url_case_det_base + case_item['url'], cookies = cookie_jar)
			
				for item in self.parse_case_details(resp, case_item['case_code']):
					yield item
			except:
				pass

	# callback for case details
	def parse_case_details(self, resp, case_code):
		cookie_jar = self.create_cookie_jar_from_resp(resp)

		select = Selector(text=resp.content.decode("utf-8"))

		related_title = select.xpath("//table[4]/caption/div/text()").extract_first()
		starting_table_ind = 5 if related_title == "Related Case Information" else 4

		# iterate over each defendant
		for defendant in select.xpath("//table[{table_ind}]/tr[./th[1]/text() = 'Defendant']".format(table_ind = starting_table_ind)):
			loader = ItemLoader(CasePartyItem(), defendant)

			loader.default_output_processor = Join()

			loader.add_value('party_side','Defendant')
			loader.add_value('case_code', case_code)
			loader.add_xpath('name', "string(.//th[2])")
			loader.add_xpath('addr', ".//following-sibling::tr[1]/td/text()")
			loader.add_xpath('removed', ".//following-sibling::tr[1]/td/nobr[contains(./text(),'Removed')]/text()", re="Removed:\\s(.*)$") # sometimes included, use regexp to clean
			loader.add_xpath('others', ".//following-sibling::tr[1]/td/nobr[contains(./text(),'Et Al')]/text()") # sometimes included

			yield loader.load_item()

		# iterate over each agent
		for agent in select.xpath(
				"//table[{table_ind}]/tr[./th[1]/text() = 'Agent']".format(table_ind=starting_table_ind)):
			loader = ItemLoader(CasePartyItem(), agent)

			loader.default_output_processor = Join()

			loader.add_value('party_side', 'Agent')
			loader.add_value('case_code', case_code)
			loader.add_xpath('name', "string(.//th[2])")
			loader.add_xpath('addr', ".//following-sibling::tr[1]/td/text()")
			loader.add_xpath('removed', ".//following-sibling::tr[1]/td/nobr[contains(./text(),'Removed')]/text()",
							 re="Removed:\\s(.*)$")  # sometimes included, use regexp to clean
			loader.add_xpath('others',
							 ".//following-sibling::tr[1]/td/nobr[contains(./text(),'Et Al')]/text()")  # sometimes included

			yield loader.load_item()

		# iterate over each plaintiff
		for plaintiff in select.xpath("//table[{table_ind}]/tr[./th[1]/text() = 'Plaintiff']".format(table_ind = starting_table_ind)):
			loader = ItemLoader(CasePartyItem(), plaintiff)

			loader.default_output_processor = Join()

			loader.add_value('party_side','Plaintiff')
			loader.add_value('case_code', case_code)
			loader.add_xpath('name', "string(.//th[2])")
			loader.add_xpath('addr', ".//following-sibling::tr[1]/td/text()")
			loader.add_xpath('removed', ".//following-sibling::tr[1]/td/nobr[contains(./text(),'Removed')]/text()", re="Removed:\\s(.*)$") # sometimes included, use regexp to clean
			loader.add_xpath('others', ".//following-sibling::tr[1]/td/nobr[contains(./text(),'Et Al')]/text()") # sometimes included

			yield loader.load_item()

		# iterate over each lawyer
		for case_party_row in select.xpath("//table[{table_ind}]/tr[./th[1]/text() = 'Plaintiff' or ./th[1]/text() = 'Defendant']".format(table_ind = starting_table_ind)):
			# get all of the non-struck-out lawyers
			lawyer_names = case_party_row.xpath(".//td[3]/b/text()").extract()
			lawyer_names.extend(case_party_row.xpath(".//td[3]/table/following-sibling::text()[1]").extract()) # add all other lawyers listed on page (sometimes multiple for a single plaintiff/defendant)
			lawyer_names = [name.title() for name in lawyer_names]
			lawyer_statuses = case_party_row.xpath(".//td[3]/i/text()").extract()

			# get all of the lawyers whose names have been struck out
			lawyer_striked_names = case_party_row.xpath(".//td[3]/b/s/text()").extract()
			lawyer_striked_names.extend(case_party_row.xpath(".//td[3]/table/following-sibling::s[1]/text()").extract()) # add all other lawyers listed on page (sometimes multiple for a single plaintiff/defendant)
			lawyer_striked_names = [name.title() for name in lawyer_striked_names]
			lawyer_striked_statuses = case_party_row.xpath(".//td[3]/i/s/text()").extract()

			party_name = case_party_row.xpath("string(.//th[2])").extract_first()

			for name, status in zip(lawyer_names, lawyer_statuses):
				yield LawyerItem(name = name, status = status, party_name = party_name, case_code = case_code, striked = False)

			for name, status in zip(lawyer_striked_names, lawyer_striked_statuses):
				yield LawyerItem(name = name, status = status, party_name = party_name, case_code = case_code, striked = True)

		# iterate over each judgment
		for judgment in select.xpath("//table[{table_ind}]/tr[contains(./td[3]/b/text(),'Judgment -')]".format(table_ind = starting_table_ind + 1)):
			loader = ItemLoader(JudgmentItem(), judgment)

			loader.default_output_processor = Join()

			loader.add_value('case_code', case_code)
			loader.add_xpath('case_type', ".//td[3]/b/text()")
			loader.add_xpath('date', ".//th/text()")
			loader.add_xpath('party', ".//td[3]/div/table/tr/td/nonobr/table/tr/td/nobr/text()")
			
			# multiple ways that the decision is formatted
			decision = judgment.xpath(".//td[3]/div/table/tr/td/nonobr/table/tr/td/text()").extract_first()

			if decision is None: # if a comment
				decision = judgment.xpath(".//td[3]/div/table/tr/td/text()").extract_first()

			if decision is None: # if a monetary award
				decision = judgment.xpath(".//td[3]/div/table[2]/tr[1]/td/text()").extract_first()

			loader.add_value('decision', decision)

			yield loader.load_item()

		for event in select.xpath("//tr[./th/text() = 'OTHER EVENTS AND HEARINGS']/following-sibling::tr"):
			loader = ItemLoader(EventItem(), event)

			loader.default_output_processor = Join()

			loader.add_value('case_code', case_code)
			loader.add_xpath('date',"./th[1]/text()")
			
			title = event.xpath(".//td[3]/b/text()").extract_first() # check for title without link
			
			if title is None: 
				title = event.xpath(".//td[3]/b/a/text()").extract_first() # for title with link

			if title is None: 
				title = event.xpath(".//td[3]/a/b/text()").extract_first() # for title with link (reversed nesting)

			loader.add_value('title', title)

			signed_issued = event.xpath(".//td[3]/div/table/tr/th/text()").extract_first() # check for officers in pattern without link

			if signed_issued is not None and "Signed" in signed_issued:
				loader.add_xpath('signed_date',".//td[3]/div/table/tr/td/text()")
			elif signed_issued is not None and "Issued" in signed_issued:
				loader.add_xpath('issued_date',".//td[3]/div/table/tr/td/text()")

			loader.add_xpath('creation_date',".//td[3]/div[contains(./text(),'Created:')]/text()", re="Created:\\s(.*)$")
			
			status = event.xpath(".//td[3]/div[1]/i/text()").extract_first() # check for status pattern without a date

			if status is None:
				status = event.xpath(".//td[3]/table/tr/th/i/text()").extract_first() # check for status pattern with a date

			loader.add_value('status',status)
			loader.add_xpath('status_date',".//td[3]/table/tr/th/i/text()")

			officer_line = event.xpath(".//td[3]/a/following-sibling::text()").extract_first() # check for officers in pattern without link
			
			if officer_line is None:
				officer_line = event.xpath(".//td[3]/b/following-sibling::text()").extract_first() # check for officers in pattern with link

			if officer_line is not None:
				time = re.findall(r"\((\d*:.*)\)", officer_line)
				time = time[0] if len(time) > 0 else None
				officer = re.findall(r"Officer:?([\s\S]*)\)", officer_line)
				officer = officer[0].strip() if len(officer) > 0 else None

				loader.add_value('officer', officer)
				loader.add_value('time', time)

			loader.add_xpath('result',".//td[3]/table/following-sibling::text()", re="Result:\\s(.*)$")
			loader.add_xpath('link',".//td[3]/b/a/@href")
			loader.add_xpath('canceled',".//td[3]/i[./text() = 'CANCELED']/text()")

			yield loader.load_item()

		tree = html.fromstring(resp.content)
		# # follow link for the first document listed

		doc_links = tree.xpath("//table[{table_ind}]/tr/td/b/a/@href".format(table_ind = starting_table_ind + 1))

		if len(doc_links) > 0:
			first_doc_link = doc_links[0]

			try:
				sleep(self.sleep_delay)
				resp = requests.post(url= self.url_case_det_base + first_doc_link, cookies = cookie_jar, headers = {'referer': resp.url})

				for item in self.parse_case_doc_page(resp, case_code):
					yield item
			except:
				pass

	# callback for processing URL for file
	def parse_case_doc_page(self, resp, case_code):
		tree = html.fromstring(resp.content)

		doc_links = tree.xpath("//table/tr/td/a/@href")
		doc_text = tree.xpath("//table/tr/td/a/text()")

		# # yield an item that will be queued for download by Scrapy's files pipeline
		yield FileItem(case_code = case_code,
			file_urls = [self.url_case_det_base + link for link, text in zip(doc_links, doc_text) if "Complaint" in text])


#process = CrawlerProcess()
#process.crawl(OJDEvictions)
#process.start()