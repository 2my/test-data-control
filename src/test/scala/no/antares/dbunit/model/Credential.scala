/* Credential.scala
   Copyright 2011 Tommy Skodje (http://www.antares.no)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package no.antares.dbunit.model

import java.util.Date;

import org.apache.commons.logging.LogFactory
import org.codehaus.jettison.json.JSONObject
;

/** Slightly complex model class */
@serializable
case class Credential {
	val serialVersionUID = -6940950737523845535L;

	val logger = LogFactory.getLog( getClass() );
  if (logger.isTraceEnabled())
		logger.trace("New CredentialData instance created.");


	var user: String	= null; // Username for login
	var password: String	= null; // password for login
	var credentialGroup: String	= null; // Credential group, i.e customer service or shop
	var userPartner: String	= null;

	var validTo: Date	= null;			  	// The date the password will expire	
	var active = true;    	// indicates The user is active
	var temporary = true; 	// indicates the password is temporary

	var name: String	= null; // Name of user
	var email: String	= null;
	var changed: Date	= null;		      	// The date the user has been changed last time

	var compNo: String	= null; 		// Company number that user is assigned to
	var compName: String	= null; 	// Company name that user is assigned to

	var partner: String	= null;

	var startPage: String	= null; 			// Which page should user be routed to when
										// logging in? Only for partner users (values in 'SCORE' and 'PORTAL')

	var defaultProduct: String	= null; // Default product for user

	// private Collection<ProductData> products;      	// Allowed products for this user

	var menuShown = true;

}

object Credential {
  def sqlSelectAll	= "SELECT * FROM credentialz"
  def sqlDropScript	= "drop table credentialz;"
	def sqlCreateScript	= """create table credentialz (
	user_name varchar (30) PRIMARY KEY, 
	pass_word varchar (30), 
	name varchar (60), 
	comp_no integer, 
	default_product varchar (30), 
	credential_group varchar (30), 
	partner varchar (60), 
	start_page varchar (60) default 'SCORE', 
	changed date,	-- default sysdate, 
	valid_to date, 
	active varchar (1) default 'Y', 
	temporari varchar (1) default 'Y', 
	email varchar (50)
);
""";

  val userNameFromFile = "TestUser1"
  val userNameFromXml = "TestUser2"

  def flatXmlTestData	= """<?xml version='1.0' encoding='UTF-8'?>
<dataset>
  <CREDENTIALZ
   USER_NAME="TestUser2"
   PASS_WORD="TestPwd1"
   NAME="Test User One"
   COMP_NO="1"
   DEFAULT_PRODUCT="TestProdukt" CREDENTIAL_GROUP="TEST" PARTNER="TEST" START_PAGE="TESTPAGE"
   CHANGED="2007-01-22 08:54:18.0"
   ACTIVE="Y"
   TEMPORARI="N"
   />
</dataset>"""
;

  def jsonTestData	= """{
	"dataset": {
		"CREDENTIALZ": [{
			"USER_NAME": "TestUser1",
			"PASS_WORD": "TestPwd1",
			"NAME": "Test User One",
			"COMP_NO": 1,
			"DEFAULT_PRODUCT": "TestProdukt",
			"CREDENTIAL_GROUP": "TEST",
			"PARTNER": "TEST",
			"START_PAGE": "TESTPAGE",
			"CHANGED": "2007-01-22 08:54:18.0",
			"ACTIVE": "Y",
			"TEMPORARI": "N"
		}
  }"""
;

  def from( jsonO: JSONObject ): Credential = {
    val user  = new Credential
    user.user = jsonO.getString( "@USER_NAME" )
    user
  }

}