CREATE OR REPLACE EDITIONABLE PACKAGE BODY "APPS"."XXD_CST_DUTY_ELE_INB_TR_PKG" 
IS
		/******************************************************************************************
			NAME           : XXD_CST_DUTY_ELE_INB_TR_PKG
			PROGRAM NAME   : Deckers TRO Inbound Duty Elements Upload

			REVISIONS:
			Date        Author             Version  Description
			----------  ----------         -------  ---------------------------------------------------
			13-AUG-2021 Damodara Gupta     1.0      Created this package using XXD_CST_DUTY_ELE_INB_TR_PKG
																																											to load the Duty/Cost Elements into the staging table,
                                           upload the Cost Elements into the XXDO_INVVAL_DUTY_COST 
																																											staging table and process to ORACLE
   25-FEB-2022 Damodara Gupta     1.1      CCR0009885
   18-JUL-2022 Damodara/Ramesh    1.2      CCR0010051 - Primary Duty Flag Update 
   30-JAN-2023 Thirupathi Gajula  1.3      CCR0010355 - COO Preference Flag Update
		*********************************************************************************************/ 
	PROCEDURE write_log_prc (pv_msg    IN    VARCHAR2)
	IS
	/****************************************************
	-- PROCEDURE write_log_prc
	-- PURPOSE: This Procedure write the log messages 
	*****************************************************/
	  lv_msg VARCHAR2(4000)  := pv_msg;
	BEGIN
						IF gn_user_id = -1
						THEN
										dbms_output.put_line(pv_msg);
						ELSE
										apps.fnd_file.put_line(apps.fnd_file.log, pv_msg);
						END IF;
	EXCEPTION
					 WHEN OTHERS 
					 THEN
									apps.fnd_file.put_line (apps.fnd_file.log, 'Error in write_log_prc Procedure -'||SQLERRM);
									dbms_output.put_line ('Error in write_log_prc Procedure -'||SQLERRM);
	END write_log_prc;

	/**********************************************************************
	-- FUNCTION xxd_remove_junk_fnc
	-- PURPOSE: This Procedure Removes Chr (9), Chr(10), Chr(13) Charcters 
	**********************************************************************/
	FUNCTION xxd_remove_junk_fnc( p_input    IN  VARCHAR2	)
 RETURN VARCHAR2
	IS
					lv_output VARCHAR2(32767) := NULL;
			BEGIN
								IF p_input IS NOT NULL 
								THEN
												SELECT REPLACE(REPLACE(REPLACE (REPLACE (REPLACE (p_input, CHR (9), ''), CHR (10),''),'|',' '), CHR (13), ''),',','')
														INTO lv_output
														FROM DUAL;
								ELSE
												RETURN NULL;
								END IF;
								RETURN lv_output;
			EXCEPTION
							WHEN OTHERS THEN
											RETURN NULL;
	END xxd_remove_junk_fnc;

	/***************************************************************************
	-- PROCEDURE load_file_into_tbl_prc
	-- PURPOSE: This Procedure read the data from a CSV file.
	-- And load it into the target oracle table.
	-- Finally it renames the source file with date.
	--
	-- PV_FILENAME
	-- The name of the flat file(a text file)
	--
	-- PV_DIRECTORY
	-- Name of the directory where the file is been placed.
	-- Note: The grant has to be given for the user to the directory
	-- before executing the function
	--
	-- PV_IGNORE_HEADERLINES:
	-- Pass the value as '1' to ignore importing headers.
	--
	-- PV_DELIMITER
	-- By default the delimiter is used as '|'
	-- As we are using CSV file to load the data into oracle
	--
	-- PV_OPTIONAL_ENCLOSED
	-- By default the optionally enclosed is used as '"'
	-- As we are using CSV file to load the data into oracle
	--
	**************************************************************************/	
 PROCEDURE get_file_names (pv_directory_name IN VARCHAR2)
 AS
	 	LANGUAGE JAVA
		 -- NAME 'DirList.getList( java.lang.String )';
			NAME 'XXD_UTL_FILE_LIST.getList( java.lang.String )' ;

	PROCEDURE load_file_into_tbl_prc (pv_table                IN VARCHAR2,
																																			pv_dir                  IN VARCHAR2 DEFAULT 'XXD_CST_DUTY_ELE_INB_DIR',
																																			pv_filename             IN VARCHAR2,
																																			pv_ignore_headerlines   IN INTEGER DEFAULT 1,
																																			pv_delimiter            IN VARCHAR2 DEFAULT '|',
																																			pv_optional_enclosed    IN VARCHAR2 DEFAULT '"',
																																			pv_num_of_columns       IN NUMBER)
 IS
			l_input       UTL_FILE.file_type;

			l_lastLine    VARCHAR2 (4000);
			l_cnames      VARCHAR2 (4000);
			l_bindvars    VARCHAR2 (4000);
			l_status      INTEGER;
			l_cnt         NUMBER DEFAULT 0;
			l_rowCount    NUMBER DEFAULT 0;
			l_sep         CHAR (1) DEFAULT NULL;
			l_errmsg      VARCHAR2 (4000);
			v_eof         BOOLEAN := FALSE;
			l_theCursor   NUMBER DEFAULT DBMS_SQL.open_cursor;
			v_insert      VARCHAR2 (4000);
			buffer_size   CONSTANT INTEGER := 32767;   

  	BEGIN
								write_log_prc ('Load Data Process Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
								l_cnt := 1;

								FOR TAB_COLUMNS	IN (SELECT column_name, data_type
																														FROM all_tab_columns
																													WHERE 1 = 1
																															AND table_name = pv_table AND column_id < pv_num_of_columns
																										ORDER BY column_id)
								LOOP
												l_cnames := l_cnames || tab_columns.column_name || ',';

												l_bindvars :=		l_bindvars||
																										 CASE
																																	WHEN tab_columns.data_type IN ('DATE', 'TIMESTAMP(6)')
																																	THEN
																																					':b' || l_cnt || ','
																																	ELSE
																																					':b' || l_cnt || ','
																													END;

												l_cnt := l_cnt + 1;
								END LOOP;

								l_cnames := RTRIM (l_cnames, ',');
								l_bindvars := RTRIM (l_bindvars, ',');

								write_log_prc ('Count of Columns is - ' || l_cnt);        

								l_input := UTL_FILE.FOPEN (pv_dir, pv_filename, 'r',buffer_size);

								IF pv_ignore_headerlines > 0
								THEN
												BEGIN
																	FOR i IN 1 .. pv_ignore_headerlines
																	LOOP
																					write_log_prc ('No of lines Ignored is - ' || i);
																					UTL_FILE.get_line (l_input, l_lastLine);
																	END LOOP;
												EXCEPTION
																 WHEN NO_DATA_FOUND
																 THEN
																			 	v_eof := TRUE;
												END;
								END IF;

								v_insert := 'insert into '
																				|| pv_table
																				|| '('
																				|| l_cnames
																				|| ') values ('
																				|| l_bindvars
																				|| ')';

								IF NOT v_eof
								THEN
													write_log_prc (l_thecursor
																								|| '-'
																								|| 'insert into '
																								|| pv_table
																								|| '('
																								|| l_cnames
																								|| ') values ('
																								|| l_bindvars
																								|| ')'); 

												DBMS_SQL.parse (l_thecursor, v_insert, DBMS_SQL.native);

												LOOP
																BEGIN
																				 UTL_FILE.GET_LINE (l_input, l_lastLine);
																EXCEPTION
																			 	WHEN NO_DATA_FOUND
																				 THEN
																					 			EXIT;
																END;

																IF LENGTH (l_lastLine) > 0
																THEN
																				FOR i IN 1 .. l_cnt - 1
																				LOOP

																								DBMS_SQL.bind_variable (
																												l_theCursor,
																												':b' || i,
																												xxd_remove_junk_fnc(RTRIM (
																																RTRIM (
																																				LTRIM (
																																								LTRIM (
																																												REGEXP_SUBSTR (
																																																l_lastline,
																																															--'(^|,)("[^"]*"|[^",]*)',
																																																'([^|]*)(\||$)',
																																																1,
																																																i),
																																												pv_delimiter),
																																								pv_optional_enclosed),
																																				pv_delimiter),
																																pv_optional_enclosed)));
																				END LOOP;

																				BEGIN
																		 						l_status := DBMS_SQL.execute (l_theCursor);

																				 				l_rowcount := l_rowcount + 1;
																				EXCEPTION
																						 		WHEN OTHERS
																							 	THEN
																												 l_errmsg := SQLERRM;
																				END;
																END IF;
												END LOOP;

												DBMS_SQL.close_cursor (l_theCursor);
												UTL_FILE.fclose (l_input);

								END IF;

 EXCEPTION
      WHEN OTHERS
	     THEN
	 	      write_log_prc ('Error in load_file_into_tbl_prc Procedure -'||SQLERRM);
	END load_file_into_tbl_prc;

	/***************************************************************************
	-- PROCEDURE validate_prc
	-- PURPOSE: This Procedure validate the recoreds present in staging table.
	****************************************************************************/ 

	PROCEDURE validate_prc (pv_file_name VARCHAR2
	                       -- ,pv_duty_override VARCHAR2)
																								)
	IS
	  CURSOR fecth_tr_duty_elements
			IS
			  SELECT ROWID,
					       style_number,
												style_description,
												country_of_origin,
												hts_code,
												duty_rate,
												additional_duty,
												applicable_uom,
												destination_country,
												preferential_rate,
												pr_start_date,
												pr_end_date,
												default_duty_rate,
												default_additional_duty,
												default_applicable_uom,
												effective_start_date,
												effective_end_date,
												department,
												data_source,
												other_vendor_sites,
												intro_season,
												size_run,
												pd_assigned,
												collection,
												tarrif_description,
												itemcategory,
--												currency_code,
												additional_field1,
												additional_field2,
												additional_field3,
												additional_field4,
												additional_field5,
												additional_field6,
												additional_field7,
												additional_field8,
												additional_field9,
												additional_field10,
												additional_field11,
												additional_field12,
												additional_field13,
												additional_field14,
												additional_field15,
												additional_field16,
												additional_field17,
												additional_field18,
												additional_field19,
												additional_field20,
					       region,
												country,
												country_code,
												operating_unit,
												organization_code,
												inventory_org_id,
												coo_precedence,
												coo_preference_flag,
												active_flag,
												preferential_duty_flag,
												rec_status,
												error_msg,
												request_id,
												filename
							FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t
						WHERE 1 = 1
						  AND rec_status = 'N'
						  AND request_id = gn_request_id
						  AND UPPER (filename) = UPPER (pv_file_name)
			ORDER BY style_number,
												country_of_origin,
												destination_country;

   TYPE tb_rec IS TABLE OF fecth_tr_duty_elements%ROWTYPE
			INDEX BY BINARY_INTEGER;

			v_tb_rec      tb_rec;

			v_bulk_limit              NUMBER := 5000;
			lv_country_of_origin      VARCHAR2 (2);
			lv_destination_country    VARCHAR2 (2);
			lv_style_nuber            VARCHAR2(100);
			lv_currency_code          VARCHAR2(100);
   e_bulk_errors             EXCEPTION;
   PRAGMA EXCEPTION_INIT     (e_bulk_errors, -24381);
			l_msg                     VARCHAR2 (4000);
   l_idx                     NUMBER;
			l_error_count             NUMBER;
			lv_vs_dest_country        VARCHAR2 (2);
			lv_department             VARCHAR2 (100);

	  BEGIN
								write_log_prc ('Validate PRC Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

								OPEN fecth_tr_duty_elements;
								v_tb_rec.DELETE;

             LOOP
                 FETCH fecth_tr_duty_elements
                       BULK COLLECT INTO v_tb_rec
                       LIMIT v_bulk_limit;

                 EXIT WHEN v_tb_rec.COUNT = 0;

                 IF v_tb_rec.COUNT > 0
																	THEN
																	    write_log_prc ('Record Count: '||v_tb_rec.COUNT);
                    BEGIN

																	        FOR i IN 1 .. v_tb_rec.COUNT
																									LOOP

																													IF v_tb_rec(i).style_number IS NOT NULL
																													THEN
																													    BEGIN
																																	     -- write_log_prc ('Style Number: '||v_tb_rec(i).style_number);
																																						SELECT style_number
																																								INTO lv_style_nuber
																																								FROM apps.xxd_common_items_v
																																							WHERE  1 = 1
																                         AND style_number = v_tb_rec(i).style_number
                                         -- AND NVL (TRUNC (start_date_active), TRUNC (SYSDATE)) <= TRUNC (SYSDATE)
                                         -- AND NVL (TRUNC (end_date_active), TRUNC (SYSDATE)) >= TRUNC (SYSDATE)
																																									-- AND enabled_flag = 'Y'
																																									AND ROWNUM = 1;

																																						IF lv_style_nuber <> v_tb_rec(i).style_number
																																						THEN
																																						   v_tb_rec(i).rec_status := 'E';
																																									v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Style Number is not Valid-';
																																						END IF;

																											      EXCEPTION
																																	 				WHEN OTHERS THEN
																																		 								write_log_prc ('Style Number'||SQLERRM);
																																											v_tb_rec(i).rec_status := 'E';
																																											v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Style Number is not Valid-';
																																 END;
																													ELSIF v_tb_rec(i).style_number IS NULL
																													THEN
																													   v_tb_rec(i).rec_status := 'E';
																													   v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Style Number Cannnot be Null-';
																													END IF;

																													IF v_tb_rec(i).country_of_origin IS NOT NULL
																													THEN
																																	BEGIN
                                      -- write_log_prc ('Country Of Origin: '||v_tb_rec(i).country_of_origin);
																																						SELECT territory_code
																																								INTO lv_country_of_origin
																																								FROM apps.fnd_territories
																																							WHERE 1 = 1
																                         AND territory_code = v_tb_rec(i).country_of_origin;

																																							IF lv_country_of_origin <> v_tb_rec(i).country_of_origin
																																							THEN
																																							   v_tb_rec(i).rec_status := 'E';
																																										v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Country Of Origin is not Valid-';
																																							END IF;

																																	EXCEPTION
																																						WHEN OTHERS THEN
																																											write_log_prc ('Country of Origin'||SQLERRM);
																																											v_tb_rec(i).rec_status := 'E';
																																											v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Country Of Origin is not Valid-';
																																	END;
																													ELSIF v_tb_rec(i).country_of_origin IS NULL
																													THEN
																													   v_tb_rec(i).rec_status := 'E';
																													   v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Country of Origin Cannnot be Null-';
 																												END IF;

																													IF v_tb_rec(i).destination_country IS NOT NULL
																													THEN
																																		BEGIN

																																							SELECT ffvl.description
																																									INTO lv_vs_dest_country
																																									FROM apps.fnd_flex_value_sets  fvs,
																																														apps.fnd_flex_values_vl   ffvl
																																								WHERE fvs.flex_value_set_id = ffvl.flex_value_set_id
																																										AND fvs.flex_value_set_name = 'XXD_CM_TR_COUNTRY_MAP_VS'
																																										AND NVL (TRUNC (ffvl.start_date_active),TRUNC (SYSDATE)) <=TRUNC (SYSDATE)
																																										AND NVL (TRUNC (ffvl.end_date_active),TRUNC (SYSDATE)) >=TRUNC (SYSDATE)
																																										AND ffvl.enabled_flag = 'Y'
																																										AND ffvl.flex_value = v_tb_rec(i).destination_country;

																																										-- write_log_prc (lv_vs_dest_country||'-'||v_tb_rec(i).destination_country);
																																										v_tb_rec(i).destination_country := lv_vs_dest_country;

																																		EXCEPTION
																																							WHEN OTHERS
																																							THEN
																																											write_log_prc ('TR Destination Country not available in Value Set - '||v_tb_rec(i).destination_country);
																																											lv_vs_dest_country := NULL;
																																		END;

																																		BEGIN
																																							SELECT territory_code
																																									INTO lv_destination_country
																																									FROM apps.fnd_territories
																																								WHERE 1 = 1
																																										AND territory_code = v_tb_rec(i).destination_country;

																																							IF lv_destination_country <> v_tb_rec(i).destination_country
																																							THEN
																																										v_tb_rec(i).rec_status := 'E';
																																										v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Destination Country is not Valid-';

																																							END IF;

																																		EXCEPTION
																																							WHEN OTHERS THEN
																																												write_log_prc ('Destination Country'||SQLERRM);
																																												v_tb_rec(i).rec_status := 'E';
																																												v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Destination Country is not Valid-';
																																		END;

																												 ELSIF v_tb_rec(i).destination_country IS NULL
																													THEN
																																	v_tb_rec(i).rec_status := 'E';
																																	v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Destination Country Cannnot be Null-';
 																												END IF;

	/*																													IF v_tb_rec(i).destination_country IS NOT NULL
																													THEN
																																	BEGIN
																																	     -- write_log_prc ('Destination Country: '||v_tb_rec(i).destination_country);
																																						SELECT territory_code
																																								INTO lv_destination_country
																																								FROM apps.fnd_territories
																																							WHERE 1 = 1
																																							  AND territory_code = v_tb_rec(i).destination_country;

																																							IF lv_destination_country <> v_tb_rec(i).destination_country
																																							THEN
																																							   v_tb_rec(i).rec_status := 'E';
																																										v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Destination Country is not Valid-';

																																							ELSIF lv_destination_country = v_tb_rec(i).destination_country
																																							THEN
																																							     BEGIN

																																																	SELECT ffvl.description
																																																			INTO lv_vs_dest_country
																																																			FROM apps.fnd_flex_value_sets  fvs,
																																																								apps.fnd_flex_values_vl   ffvl
																																																		WHERE fvs.flex_value_set_id = ffvl.flex_value_set_id
																																																				AND fvs.flex_value_set_name = 'XXD_CM_TR_COUNTRY_MAP_VS'
																																																				AND NVL (TRUNC (ffvl.start_date_active),TRUNC (SYSDATE)) <=TRUNC (SYSDATE)
																																																				AND NVL (TRUNC (ffvl.end_date_active),TRUNC (SYSDATE)) >=TRUNC (SYSDATE)
																																																				AND ffvl.enabled_flag = 'Y'
																																																				AND ffvl.flex_value = v_tb_rec(i).destination_country;

																																																				-- write_log_prc (lv_vs_dest_country||'-'||v_tb_rec(i).destination_country);
																																																				v_tb_rec(i).destination_country := lv_vs_dest_country;

																																												EXCEPTION
                                                 WHEN OTHERS
																																																	THEN
																																																	    write_log_prc ('TR Destination Country not available in Value Set');
																																																	    lv_vs_dest_country := NULL;
																																												END;

																																							END IF;

																																	EXCEPTION
																																						WHEN OTHERS THEN
																																											write_log_prc ('Destination Country'||SQLERRM);
																																							    v_tb_rec(i).rec_status := 'E';
																																										 v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Destination Country is not Valid-';
																																	END;
																												 ELSIF v_tb_rec(i).destination_country IS NULL
																													THEN
																													   v_tb_rec(i).rec_status := 'E';
																													   v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Destination Country Cannnot be Null-';
 																												END IF;*/

																													-- Need to Verify If effective start date can be backed date/Future Date from Inbound file

																													IF TO_DATE (v_tb_rec(i).effective_start_date,'YYYY-MM-DD') = TO_DATE ('1900-01-01', 'YYYY-MM-DD')
																													THEN
																													   v_tb_rec(i).effective_start_date := '';
																													END IF;

																													IF TO_DATE (v_tb_rec(i).effective_end_date,'YYYY-MM-DD') = TO_DATE ('1900-01-01', 'YYYY-MM-DD')
																													THEN
																													   v_tb_rec(i).effective_end_date := '';
																													END IF;

																									    IF v_tb_rec(i).effective_start_date IS NULL
																												 THEN
																													   v_tb_rec(i).effective_start_date := TO_CHAR (TRUNC (SYSDATE), 'YYYY-MM-DD');
																													   write_log_prc ('Assigned SYSDATE since Effective Start Date is NULL or Effective Start Date is 1900-01-01'); 
																																write_log_prc (v_tb_rec(i).effective_start_date);
																												 END IF;

																									    IF NVL(TO_DATE (v_tb_rec(i).effective_end_date,'YYYY-MM-DD'), TRUNC (SYSDATE)) < TO_DATE (v_tb_rec(i).effective_start_date,'YYYY-MM-DD')
																												 THEN
																													   v_tb_rec(i).rec_status := 'E';
                                v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Effective End Date Cannnot Lesser than Effective Start Date-';
																													   write_log_prc ('Effective End Date Cannnot Lesser than Effective Start Date'); 
																												 END IF;

																									    IF NVL(TO_DATE (v_tb_rec(i).effective_end_date,'YYYY-MM-DD'), TRUNC (SYSDATE)) < TRUNC (SYSDATE)
																												 THEN
																													   v_tb_rec(i).rec_status := 'E';
                                v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Effective End Date Cannnot Lesser than Sysdate-';
																													   write_log_prc ('Effective End Date Cannnot Lesser than Sysdate'); 
																												 END IF;

																													IF v_tb_rec(i).preferential_rate = 'Y'
																													THEN

																																	IF TO_DATE (v_tb_rec(i).pr_start_date,'YYYY-MM-DD') = TO_DATE ('1900-01-01', 'YYYY-MM-DD')
																																	THEN
																																				v_tb_rec(i).pr_start_date := '';
																																	END IF;

																																	IF TO_DATE (v_tb_rec(i).pr_end_date,'YYYY-MM-DD') = TO_DATE ('1900-01-01', 'YYYY-MM-DD')
																																	THEN
																																				v_tb_rec(i).pr_end_date := '';
																																	END IF;

																																	IF v_tb_rec(i).pr_start_date IS NULL
																																	THEN
																																				v_tb_rec(i).pr_start_date := TO_CHAR (TRUNC (SYSDATE), 'YYYY-MM-DD');
																																				write_log_prc ('Assigned SYSDATE since PR Start Date is NULL or PR Start Date is 1900-01-01'); 
																																				write_log_prc (v_tb_rec(i).pr_start_date);
																																	END IF;

																																	IF NVL(TO_DATE (v_tb_rec(i).pr_end_date,'YYYY-MM-DD'), TRUNC (SYSDATE)) < TO_DATE (v_tb_rec(i).pr_start_date,'YYYY-MM-DD')
																																	THEN
																																				v_tb_rec(i).rec_status := 'E';
																																				v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'PR End Date Cannnot Lesser than PR Start Date-';
																																				write_log_prc ('PR End Date Cannnot Lesser than PR Start Date'); 
																																	END IF;

																																	IF NVL(TO_DATE (v_tb_rec(i).pr_end_date,'YYYY-MM-DD'), TRUNC (SYSDATE)) < TRUNC (SYSDATE)
																																	THEN
																																				v_tb_rec(i).rec_status := 'E';
																																				v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'PR End Date Cannnot Lesser than Sysdate-';
																																				write_log_prc ('PR End Date Cannnot Lesser than Sysdate'); 
																																	END IF;

																													END IF;

																									    IF v_tb_rec(i).hts_code IS NULL
																												 THEN
																													   v_tb_rec(i).rec_status := 'E';
																													   v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'HTS Code Cannnot be Null-';
																												 END IF;

																													-- IF pv_duty_override = 'N' AND v_tb_rec(i).preferential_rate = 'Y'
																													-- THEN

																																	-- IF (v_tb_rec(i).duty_rate IS NULL) OR (LENGTH(TRIM(TRANSLATE (v_tb_rec(i).duty_rate, ' +-.0123456789',' ')))) > 0
																																	-- THEN
																																				-- v_tb_rec(i).rec_status := 'E';
																																				-- v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'DUTY Rate Cannnot be Null OR Not Valid Value-';
																																	-- END IF;

																													-- IF v_tb_rec(i).preferential_rate = 'N'
																													-- THEN

																																	-- IF (v_tb_rec(i).default_duty_rate IS NULL) OR (LENGTH(TRIM(TRANSLATE (v_tb_rec(i).default_duty_rate, ' +-.0123456789',' ')))) > 0
																																	-- THEN
																																				-- v_tb_rec(i).rec_status := 'E';
																																				-- v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Default DUTY Rate Cannnot be Null OR Not Valid Value-';
																																	-- END IF;

																													-- END IF;

                             IF v_tb_rec(i).department IS NULL
																													THEN

																																	BEGIN
																																	      SELECT department
																																							  INTO lv_department
																																								 FROM apps.xxd_common_items_v
																																								WHERE style_number = v_tb_rec(i).style_number
																																								  AND rownum =1;

																																							v_tb_rec(i).department	:= lv_department;

																																	EXCEPTION
																																	     WHEN OTHERS
																																						THEN
																																										v_tb_rec(i).rec_status := 'E';
																																										v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Unable to derive Department from xxd_common_items_v';
																																	END;

																													END IF;

																													/*BEGIN
																																		write_log_prc ('Deriving Region,Country, Inv Org based on Destination Country');

																																		SELECT parent_flex_value_low,
																																									flex_value,
																																									attribute1, 
																																									attribute2,
																																									CASE 
																																													WHEN NVL (attribute3,'X2X') = v_tb_rec(i).country_of_origin THEN 1
																																													WHEN NVL (attribute4,'X2X') = v_tb_rec(i).country_of_origin THEN 2
																																													WHEN NVL (attribute5,'X2X') = v_tb_rec(i).country_of_origin THEN 3
																																													WHEN NVL (attribute6,'X2X') = v_tb_rec(i).country_of_origin THEN 4
																																													WHEN NVL (attribute7,'X2X') = v_tb_rec(i).country_of_origin THEN 5
																																									ELSE
																																									    1000
																																									END
																																				INTO v_tb_rec(i).region
																																								,v_tb_rec(i).country_code
																																								,v_tb_rec(i).country
																																								,v_tb_rec(i).inventory_org_id
                                        ,v_tb_rec(i).coo_precedence
																																				FROM apps.fnd_flex_value_sets ffvs,
																																									apps.fnd_flex_values_vl ffvl
																																			WHERE 1 = 1
																																					AND ffvs.flex_value_set_id = ffvl.flex_value_set_id
																																					AND ffvs.flex_value_set_name = 'XXD_CM_COUNTRY_INV_ORGS_VS'
																																					AND NVL (TRUNC (ffvl.start_date_active),TRUNC (SYSDATE)) <=TRUNC (SYSDATE)
																																					AND NVL (TRUNC (ffvl.end_date_active),TRUNC (SYSDATE)) >=TRUNC (SYSDATE)
																																					AND enabled_flag = 'Y'
																																					AND ffvl.attribute1 = v_tb_rec(i).destination_country;

																													EXCEPTION
																																		WHEN OTHERS
																																		THEN
																																						v_tb_rec(i).rec_status := 'E';
																																						v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Region, Country, Inv Org Values Does not exists in XXD_CM_COUNTRY_INV_ORGS_VS for Destination Country:'||v_tb_rec(i).destination_country||'-';
																																						write_log_prc (SQLERRM||' Failed to retrive Region, Country, Inv Org for Destination Country');
																													END;

																													BEGIN

																																		SELECT operating_unit
																																								,organization_code
																																				INTO v_tb_rec(i).operating_unit
																																								,v_tb_rec(i).organization_code
																																				FROM apps.org_organization_definitions 
																																			WHERE 1 = 1
																																					AND organization_id = v_tb_rec(i).inventory_org_id;

																																		write_log_prc ('Derived Operating Unit:'||v_tb_rec(i).operating_unit||'Organization Code:'||v_tb_rec(i).organization_code||' based on Destination Country');	

																													EXCEPTION
																																		WHEN OTHERS 
																																		THEN
																																						v_tb_rec(i).rec_status := 'E';
																																						v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Failed to Retrive Operating Unit for Inv Org:'||v_tb_rec(i).inventory_org_id||'-';
																																						write_log_prc (SQLERRM||' Failed to  Retrive Operating Unit for the Inv Org-'||v_tb_rec(i).inventory_org_id			);
																													END;*/

																									END LOOP;

																				EXCEPTION
																				     WHEN OTHERS
																									THEN
																									     NULL;
																									     write_log_prc (SQLERRM||' Other Error - Record Validations Failed');
																				END;

																				BEGIN
																									FORALL i IN v_tb_rec.first .. v_tb_rec.last SAVE EXCEPTIONS

																																UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t
																																			SET region = v_tb_rec(i).region
																																			   ,destination_country = v_tb_rec(i).destination_country
																																						,department = v_tb_rec(i).department
																																						--,country_code = v_tb_rec(i).country_code
																																						--,country = v_tb_rec(i).country
																																						--,inventory_org_id = v_tb_rec(i).inventory_org_id	
																																						--,operating_unit = v_tb_rec(i).operating_unit
																																						--,organization_code = v_tb_rec(i).organization_code
																																						--,coo_precedence = v_tb_rec(i).coo_precedence
																																			   ,effective_start_date = v_tb_rec(i).effective_start_date
																																			   ,effective_end_date = v_tb_rec(i).effective_end_date
																																			   ,pr_start_date = v_tb_rec(i).pr_start_date
																																						,pr_end_date = v_tb_rec(i).pr_end_date
    																																		,rec_status = v_tb_rec(i).rec_status
																																						,error_msg = v_tb_rec(i).error_msg
																																	WHERE ROWID = v_tb_rec(i).rowid;

																										     write_log_prc (SQL%ROWCOUNT||' Derived Values Updated...');

																				EXCEPTION
																									WHEN e_bulk_errors
																									THEN
																												write_log_prc ('Inside E_BULK_ERRORS');
																												l_error_count := SQL%BULK_EXCEPTIONS.COUNT;

																												FOR i IN 1 .. l_error_count
																												LOOP
																															l_msg := SQLERRM (-SQL%BULK_EXCEPTIONS (i).ERROR_CODE);
																															l_idx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
																															write_log_prc ('Failed to update TR Element For Style Number- '
																																														|| v_tb_rec (l_idx).style_number
																																														|| ' with error_code- '
																																														|| l_msg);
																												END LOOP;

																									WHEN OTHERS 
																									THEN
																													write_log_prc ('Update Failed for Error Records' || SQLERRM);
																				END;

																				COMMIT;

																	END IF;

													EXIT WHEN fecth_tr_duty_elements%NOTFOUND;																	
													END LOOP;

								CLOSE fecth_tr_duty_elements;

			     UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t
								   SET rec_status = 'E',
											    error_msg = error_msg||'Duplicate Record-'
         WHERE 1 = 1
									  AND (style_number, 
									       country_of_origin, 
																destination_country) IN ( SELECT style_number, 
																																															  country_of_origin, 
																																															  destination_country
                                            FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t
                                           WHERE 1 = 1 
																																											  --AND rec_status = 'N'
																																													AND request_id = gn_request_id
																																													AND filename = pv_file_name
                                        GROUP BY style_number,
																																								         country_of_origin,
																																																	destination_country
                                          HAVING COUNT (1) > 1)
                --AND rec_status = 'N'
																AND request_id = gn_request_id
                AND filename = pv_file_name;

         write_log_prc ( SQL%ROWCOUNT || ' Element records updated with error - Duplicate Records');
									COMMIT;												

									UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t t1
            SET active_flag = 'N'
          WHERE 1 = 1
										  AND active_flag = 'Y' 
										  AND (style_number, 
																 country_of_origin, 
																 destination_country)  IN (SELECT style_number, 
																																																		country_of_origin, 
																																																		destination_country
																																													FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t t2
																																												WHERE 1 = 1
																																												  -- AND rec_status = 'N'
																																														-- AND error_msg IS NULL
		 																																											AND request_id = gn_request_id
																																														AND filename = pv_file_name 
																																														AND active_flag = 'Y'
																																														AND t1.style_number = t2.style_number
																																														AND t1.country_of_origin = t2.country_of_origin
																																														AND t1.destination_country = t2.destination_country)

												-- AND error_msg IS NULL
            -- AND request_id <> gn_request_id
												AND filename <> pv_file_name;			

         write_log_prc ( SQL%ROWCOUNT || ' Element records updated with Current Flag as N ');
									COMMIT;

									-- UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t u
									   -- SET coo_preference_flag = 'Y'
									 -- WHERE EXISTS (SELECT s.style_number
																														-- ,s.region
																														-- ,s.country
																														-- ,s.inventory_org_id
																														-- ,s.coo_precedence FROM (SELECT style_number style_number
																																																												-- ,region region
																																																												-- ,country country
																																																												-- ,inventory_org_id inventory_org_id
																																																												-- ,MIN (coo_precedence) coo_precedence
																																																						  -- FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t
																																																							-- WHERE coo_precedence <> 1000
																																																				-- GROUP BY style_number
																																																												-- ,region
																																																												-- ,country
																																																												-- ,inventory_org_id) s
                         -- WHERE s.style_number = u.style_number
																											-- AND s.region = u.region
																											-- AND s.country = u.country
																											-- AND s.inventory_org_id = u.inventory_org_id
																											-- AND s.coo_precedence = u.coo_precedence
																											-- AND coo_precedence <> 1000)
												-- AND request_id = gn_request_id
            -- AND filename = pv_file_name
												-- AND coo_precedence <> 1000
												-- And active_flag = 'Y';

         -- write_log_prc ( SQL%ROWCOUNT || ' Records updated COO preference flag as Y');
									-- COMMIT;


         -- UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t
            -- SET coo_preference_flag = 'Y'
          -- WHERE (style_number, 
										       -- country_of_origin) IN (SELECT style_number,
                              																	-- country_of_origin FROM (SELECT style_number,
                               																																															-- country_of_origin, 
																																																																														-- ROW_NUMBER() OVER (PARTITION BY style_number ORDER BY style_number) rn 
																																																																								 -- FROM (SELECT style_number, 
																																																																									             -- country_of_origin 
																																																																																	-- FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t t1
																																																						                          -- WHERE coo_precedence = 1000
																																																						                            -- AND style_number NOT IN (SELECT style_number
                           																																																																																		-- FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t t2
																																																						                                                      -- WHERE coo_precedence in (1,2,3,4,5))
																																																						               -- GROUP BY style_number, country_of_origin)
																																																																					 -- )
																																																						-- WHERE rn = 1
																																							-- )
            -- AND coo_preference_flag IS NULL;

         -- write_log_prc ( SQL%ROWCOUNT || ' records updated with coo_preference_flag as Y ');
									-- COMMIT;

									UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t
									   SET rec_status = 'V'
										WHERE rec_status = 'N'
										  AND error_msg IS NULL
												AND active_flag = 'Y' 
												AND request_id = gn_request_id
												AND filename = pv_file_name;

         write_log_prc ( SQL%ROWCOUNT || ' Element records updated with Flag as V ');
									COMMIT;

								write_log_prc ('Validate PRC Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

			EXCEPTION
        WHEN OTHERS
        THEN
            write_log_prc ('Error in validate_prc Procedure -'||SQLERRM);									

	END validate_prc;

	/***************************************************************************
	-- PROCEDURE insert_cat_to_stg_prc
	-- PURPOSE: This Procedure inserts catagory records to catagory staging table
	***************************************************************************/

	PROCEDURE insert_cat_to_stg_prc (x_errbuf  OUT NOCOPY VARCHAR2,
																												    		x_retcode OUT NOCOPY VARCHAR2)
	AS
			CURSOR c_cat_main
			IS
					SELECT *
							FROM (SELECT DISTINCT item_number,
																													inventory_org_id organization_id,
																													'TARRIF CODE' category_set_name,
																													tarrif_code segment1,
																													country segment3,
																													--DEFAULT_CATEGORY segment3,
																													default_category segment4,
																													inventory_item_id inventory_item_id,
																													group_id
															FROM xxdo.xxd_cst_duty_ele_upld_stg_t
														WHERE 1 = 1
														  AND tarrif_code IS NOT NULL
																AND status_category = gc_new_status
																AND rec_status = 'P'
																AND active_flag = 'Y'
																AND error_msg IS NULL
																AND (   tarrif_code IS NOT NULL	
																     OR country IS NOT NULL 
																					OR default_category IS NOT NULL));

					TYPE c_main_type IS TABLE OF c_cat_main%ROWTYPE
																												INDEX BY BINARY_INTEGER;

					lt_main_tab      c_main_type;

					ld_date          DATE;
					ln_total_count   NUMBER;
					ln_count         NUMBER;

	BEGIN
	     write_log_prc ('Procedure insert_cat_to_stg_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
						BEGIN
											DELETE FROM xxdo.xxd_cst_duty_ele_cat_stg_t
																	WHERE category_set_name = 'TARRIF CODE';

											COMMIT;
						EXCEPTION
											WHEN OTHERS
											THEN
											write_log_prc ('Error while deleting Tarrif code category records from table xxdo.xxd_cst_duty_ele_cat_stg_t');
					 END;

						SELECT SYSDATE INTO ld_date FROM sys.DUAL;

						OPEN c_cat_main;

						LOOP
										FETCH c_cat_main
										BULK COLLECT INTO lt_main_tab
										LIMIT 20000;

										IF lt_main_tab.COUNT = 0
										THEN
														write_log_prc ('No Valid records are present in the xxdo.xxd_cst_duty_ele_upld_stg_t table  and SQLERRM' || SQLERRM);
									ELSE
														FORALL i IN 1 .. lt_main_tab.COUNT
															--Inserting to Staging Table xxdo.xxd_cst_duty_ele_cat_stg_t
														INSERT INTO xxdo.xxd_cst_duty_ele_cat_stg_t (
																																																											record_id,
																																																											batch_number,
																																																											record_status,
																																																											item_number,
																																																											organization_id,
																																																											category_set_name,
																																																											segment1,
																																																											-- SEGMENT2 ,
																																																											segment3,
																																																											segment4,
																																																											inventory_item_id,
																																																											created_by,
																																																											creation_date,
																																																											last_updated_by,
																																																											last_update_date,
																																																											error_msg,
																																																											group_id
																																																											)
																																																					VALUES 
																																																											(
																																																											xxdo.xxd_cst_duty_ele_cat_stg_t_s.NEXTVAL,
																																																											NULL,
																																																											'N',
																																																											lt_main_tab (i).item_number,
																																																											lt_main_tab (i).organization_id,
																																																											lt_main_tab (i).category_set_name,
																																																											lt_main_tab (i).segment1,
																																																											--lt_main_tab (i).SEGMENT2,
																																																											lt_main_tab (i).segment3,
																																																											lt_main_tab (i).segment4,
																																																											lt_main_tab (i).inventory_item_id,
																																																											fnd_global.user_id,
																																																											ld_date,
																																																											fnd_global.login_id,
																																																											ld_date,
																																																											NULL,
																																																											lt_main_tab (i).group_id);


														ln_total_count := ln_total_count + ln_count;
														ln_count := ln_count + 1;

														IF ln_total_count = 20000
														THEN
																	ln_total_count := 0;
																	ln_count := 0;
																	COMMIT;
														END IF;
											END IF;

									EXIT WHEN lt_main_tab.COUNT < 20000;
						END LOOP;

						CLOSE c_cat_main;

						UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t
									SET status_category = 'I'
							WHERE 1 = 1
									AND status_category = gc_validate_status
									AND (  tarrif_code IS NOT NULL 
													OR country IS NOT NULL 
													OR default_category IS NOT NULL);

						COMMIT;
						write_log_prc ('Procedure insert_cat_to_stg_prc Ends....'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
	EXCEPTION
						WHEN OTHERS
						THEN
										write_log_prc ('Error in procedure insert_cat_to_stg_prc:' || SQLERRM);
										NULL;
	END insert_cat_to_stg_prc;

	/*********************************************************
	-- PROCEDURE duty_uom_conversion_fnc
	-- PURPOSE: This Function calulate the addtional duty
	*********************************************************/
FUNCTION duty_uom_conversion_fnc (pv_inv_item_id IN VARCHAR2
																																	,pv_inv_org_id IN VARCHAR2
																																	,pv_additional_duty IN VARCHAR2
																																	,pv_applicable_uom IN VARCHAR2
																																	,pv_preferential_rate IN VARCHAR2
																																	,pv_preferential_duty_flag IN VARCHAR2
																																	,pv_default_additional_duty IN VARCHAR2
																																	,pv_default_applicable_uom IN VARCHAR2)
RETURN VARCHAR2
IS
  lv_sku_weight_uom_code        VARCHAR2 (240);
		lv_sku_unit_weight            NUMBER;
		lv_conv_uom_value             NUMBER;
		lv_sku_conv_unit_weight       NUMBER;
		lv_tot_addl_duty              NUMBER;
		lv_get_item_cost              NUMBER;
		lv_tot_addl_duty_per          NUMBER;

  BEGIN
		     lv_get_item_cost := 0;
							lv_tot_addl_duty_per := 0;
		     BEGIN

												SELECT weight_uom_code
																		,TO_NUMBER (unit_weight)
														INTO lv_sku_weight_uom_code
																		,lv_sku_unit_weight
														FROM mtl_system_items_b
													WHERE inventory_item_id = pv_inv_item_id
															AND organization_id = pv_inv_org_id;

												-- write_log_prc ('UOM, Unit Weight for the Inv Item and Org :'||lv_sku_weight_uom_code||'-'||lv_sku_unit_weight);

							EXCEPTION
							     WHEN OTHERS
												THEN
												    write_log_prc (' Retiveing UOM Values Failed for the Inv Item ID and  Inv Org ID :'||pv_inv_item_id||'-'||pv_inv_org_id);
																-- dbms_output.put_line (' Retiveing UOM Values Failed for the Inv Item ID and  Inv Org ID :'||pv_inv_item_id||'-'||pv_inv_org_id);
																lv_sku_weight_uom_code := NULL;
																lv_sku_unit_weight := NULL;
						 END;

							--IF (pv_preferential_duty_flag = 'Y' OR pv_preferential_rate = 'Y')
							IF pv_preferential_rate = 'Y'
							THEN

										IF lv_sku_weight_uom_code = pv_applicable_uom
										THEN
													lv_conv_uom_value := 1;
										ELSE
													BEGIN
																		SELECT TO_NUMBER (flv.description)
																				INTO lv_conv_uom_value
																				FROM fnd_lookup_values flv
																								,fnd_lookup_types flt
																			WHERE flt.lookup_type = 'DECKERS_UOM_CONVERSION'
																					AND flv.lookup_type = flt.lookup_type
																					AND enabled_flag = 'Y'
																					AND NVL (TRUNC (flv.start_date_active),TRUNC (SYSDATE)) <= TRUNC (SYSDATE)
																					AND NVL (TRUNC (flv.end_date_active),TRUNC (SYSDATE)) >= TRUNC (SYSDATE)
																					AND language = 'US'
																					AND flv.lookup_code = lv_sku_weight_uom_code
																					AND flv.meaning = pv_applicable_uom;

																  -- write_log_prc ('UOM Conversion Value from Lookup :'||lv_conv_uom_value);

													EXCEPTION
																		WHEN OTHERS
																		THEN
																						write_log_prc (' Retiveing UOM Weight Failed From Lookup for the SKU UOM Code and TR UOM Code :'||lv_sku_weight_uom_code||'-'||pv_applicable_uom);
																						-- dbms_output.put_line (' Retiveing UOM Weight Failed From Lookup for the SKU UOM Code and TR UOM Code :'||lv_sku_weight_uom_code||'-'||pv_applicable_uom);
																						lv_conv_uom_value :=  NULL;
													END;
									END IF;

									lv_sku_conv_unit_weight :=  ROUND (lv_conv_uom_value,6) * lv_sku_unit_weight;

									lv_tot_addl_duty := ROUND (lv_sku_conv_unit_weight,6) * pv_additional_duty;

									-- write_log_prc ('Total Addtional Duty :'||ROUND (lv_tot_addl_duty,6));

									BEGIN
														lv_get_item_cost := xxdoget_item_cost ('ITEMCOST'
																																																					,pv_inv_org_id
																																																					,pv_inv_item_id
																																																					,'N');

              lv_get_item_cost:= ROUND (lv_get_item_cost,6);
									EXCEPTION
									     WHEN OTHERS
														THEN
														    write_log_prc ('Exception occurred while calcluating the xxdoget_item_cost');
									END;

         -- write_log_prc ('Item Cost for Inv Id and Org Id:'||lv_get_item_cost);

								 IF lv_get_item_cost = 0
									THEN
									   BEGIN
												     SELECT list_price_per_unit
																	  INTO lv_get_item_cost
																	  FROM mtl_system_items_b
																		WHERE inventory_item_id = pv_inv_item_id
																		  AND organization_id = pv_inv_org_id;

															  -- write_log_prc ('List Price for Inv Id and Org Id:'||lv_get_item_cost);

												EXCEPTION
												     WHEN OTHERS
																	THEN
																	    lv_get_item_cost := 0;
																					write_log_prc ('Excpetion Occured while retiving the List Price');
												END;

									END IF;

									lv_tot_addl_duty_per := ROUND (lv_tot_addl_duty,6)/lv_get_item_cost;

									write_log_prc ('Total Addtional Duty Percentage :'||ROUND (lv_tot_addl_duty_per,6));

			     	RETURN ROUND (lv_tot_addl_duty_per,6);

									--ELSIF (pv_preferential_duty_flag = 'N' AND pv_preferential_rate = 'N')
									ELSIF pv_preferential_rate = 'N'
									THEN

											lv_tot_addl_duty := 0;

											IF lv_sku_weight_uom_code = pv_default_applicable_uom
											THEN
														lv_conv_uom_value := 1;
											ELSE
														BEGIN
																			SELECT flv.description
																					INTO lv_conv_uom_value
																					FROM fnd_lookup_values flv
																									,fnd_lookup_types flt
																				WHERE flt.lookup_type = 'DECKERS_UOM_CONVERSION'
																						AND flv.lookup_type = flt.lookup_type
																						AND enabled_flag = 'Y'
																						AND NVL (TRUNC (flv.start_date_active),TRUNC (SYSDATE)) <= TRUNC (SYSDATE)
																						AND NVL (TRUNC (flv.end_date_active),TRUNC (SYSDATE)) >= TRUNC (SYSDATE)
																						AND language = 'US'
																						AND flv.lookup_code = lv_sku_weight_uom_code
																						AND flv.meaning = pv_default_applicable_uom;

																  -- write_log_prc ('UOM Conversion Value from Lookup :'||lv_conv_uom_value);

														EXCEPTION
																			WHEN OTHERS
																			THEN
																							apps.fnd_file.put_line(apps.fnd_file.log, ' Retiveing UOM Weight Failed From Lookup for the SKU UOM Code and TR UOM Code :'||lv_sku_weight_uom_code||'-'||pv_default_applicable_uom);
																							-- dbms_output.put_line (' Retiveing UOM Weight Failed From Lookup for the SKU UOM Code and TR UOM Code :'||lv_sku_weight_uom_code||'-'||pv_default_applicable_uom);
																							lv_conv_uom_value :=  NULL;
														END;
							    END IF;

											lv_sku_conv_unit_weight :=  ROUND (lv_conv_uom_value,6) * lv_sku_unit_weight;

											lv_tot_addl_duty := ROUND (lv_sku_conv_unit_weight,6) * pv_default_additional_duty;

									  write_log_prc ('Total Addtional Duty :'||ROUND (lv_tot_addl_duty,6));

											lv_get_item_cost := xxdoget_item_cost ('ITEMCOST'
																																																		,pv_inv_org_id
																																																		,pv_inv_item_id
																																																		,'N');

           -- write_log_prc ('Item Cost for Inv Id and Org Id:'||lv_get_item_cost);

											IF lv_get_item_cost = 0
											THEN
														BEGIN
																			SELECT list_price_per_unit
																					INTO lv_get_item_cost
																					FROM mtl_system_items_b
																				WHERE inventory_item_id = pv_inv_item_id
																						AND organization_id = pv_inv_org_id;

              -- write_log_prc ('List Price for Inv Id and Org Id:'||lv_get_item_cost);

														EXCEPTION
																			WHEN OTHERS
																			THEN
																							lv_get_item_cost := 0;
																							write_log_prc ('Excpetion Occured while retiving the List Price');
														END;

									  END IF;

									  lv_tot_addl_duty_per := ROUND (lv_tot_addl_duty, 6)/lv_get_item_cost;

									  write_log_prc ('Total Addtional Duty Percentage :'||ROUND (lv_tot_addl_duty_per,6));

			     	  RETURN ROUND (lv_tot_addl_duty_per,6);					

							END IF;

		EXCEPTION
       WHEN OTHERS
							THEN
							    apps.fnd_file.put_line(apps.fnd_file.log, 'Exception Occured in Function duty_uom_conversion_fnc');
END duty_uom_conversion_fnc;

	/***************************************************************************
	-- PROCEDURE insert_into_custom_table_prc
	-- PURPOSE: This Procedure insert the duty element recoreds into xxdo.xxd_cst_duty_ele_upld_stg_t
	***************************************************************************/

	PROCEDURE insert_into_custom_table_prc -- (pv_duty_override VARCHAR2)
	IS
			CURSOR cur_style_data
			IS
     SELECT DISTINCT style_number
																				,country_of_origin
																				,destination_country
				   FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t s
						WHERE 1 = 1
						  AND rec_status = 'E'
								AND active_flag = 'Y'
								AND error_msg IN ('Style Number is not Valid-','Unable To Derive OH Values From Value Set', 'Duty Element cannot be NULL')
								AND request_id <> gn_request_id;

			CURSOR cur_tr_data
			IS
					SELECT style_number
					      ,style_description
											,country_of_origin
											,hts_code
											,duty_rate
											,SUBSTR (additional_duty,1,instr(additional_duty,' ',1)-1) additional_duty
											,applicable_uom
											,destination_country
											,preferential_rate
											,pr_start_date
											,pr_end_date
											,default_duty_rate
											,SUBSTR (default_additional_duty,1,instr(default_additional_duty,' ',1)-1) default_additional_duty
											,default_applicable_uom
											,effective_start_date
											,effective_end_date
											,department
											,data_source
											,other_vendor_sites
											,intro_season
											,size_run
											,pd_assigned
											,collection
											,tarrif_description
											,itemcategory
					      -- ,region
											-- ,country
											-- ,country_code
											-- ,operating_unit
											-- ,organization_code
											-- ,inventory_org_id
											-- ,coo_precedence
											-- ,coo_preference_flag
											,rec_status
											,error_msg
											,active_flag
											,preferential_duty_flag
											,filename
							FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t s
						WHERE 1 = 1
						  AND rec_status = 'V'
								AND active_flag = 'Y'
								AND error_msg IS NULL
								AND request_id = gn_request_id
		 ORDER BY style_number,
												country_of_origin,
												destination_country;

   CURSOR cur_derive_regn_cnty_org (pv_country_of_origin VARCHAR2,
			                                pv_destination_country VARCHAR2)
			IS
					SELECT parent_flex_value_low region,
												flex_value country_code,
												attribute1 country, 
												attribute2 inventory_org_id,
												CASE 
																WHEN NVL (attribute3,'X2X') = pv_country_of_origin THEN 1
																WHEN NVL (attribute4,'X2X') = pv_country_of_origin THEN 2
																WHEN NVL (attribute5,'X2X') = pv_country_of_origin THEN 3
																WHEN NVL (attribute6,'X2X') = pv_country_of_origin THEN 4
																WHEN NVL (attribute7,'X2X') = pv_country_of_origin THEN 5
--BEGIN:Added for CCR0010051
																WHEN NVL (attribute8,'X2X') = pv_country_of_origin THEN 6
																WHEN NVL (attribute9,'X2X') = pv_country_of_origin THEN 7
																WHEN NVL (attribute10,'X2X') = pv_country_of_origin THEN 8
																WHEN NVL (attribute11,'X2X') = pv_country_of_origin THEN 9
																WHEN NVL (attribute12,'X2X') = pv_country_of_origin THEN 10
																WHEN NVL (attribute13,'X2X') = pv_country_of_origin THEN 11
																WHEN NVL (attribute14,'X2X') = pv_country_of_origin THEN 12
																WHEN NVL (attribute15,'X2X') = pv_country_of_origin THEN 13
																WHEN NVL (attribute16,'X2X') = pv_country_of_origin THEN 14
																WHEN NVL (attribute17,'X2X') = pv_country_of_origin THEN 15
--END:Added for CCR0010051
												ELSE
																1000
												END coo_precedence
							FROM apps.fnd_flex_value_sets ffvs,
												apps.fnd_flex_values_vl ffvl
						WHERE 1 = 1
								AND ffvs.flex_value_set_id = ffvl.flex_value_set_id
								AND ffvs.flex_value_set_name = 'XXD_CM_COUNTRY_INV_ORGS_VS'
								AND NVL (TRUNC (ffvl.start_date_active),TRUNC (SYSDATE)) <=TRUNC (SYSDATE)
								AND NVL (TRUNC (ffvl.end_date_active),TRUNC (SYSDATE)) >=TRUNC (SYSDATE)
								AND enabled_flag = 'Y'
								AND ffvl.attribute1 = pv_destination_country;

			CURSOR cur_oh_ele_data (pv_region VARCHAR2, 
			                        pv_country VARCHAR2, 
																											pv_inv_org_id VARCHAR2)
			IS
					SELECT DISTINCT-- oh_val.attribute1 region,
												oh_val.attribute2 brand
												-- oh_val.attribute3 duty,
												-- oh_val.attribute4 freight,
												-- oh_val.attribute5 oh_duty,
												-- oh_val.attribute6 oh_nonduty,
												-- oh_val.attribute7 freight_duty,
												-- oh_val.attribute8 inv_org_id,
												-- oh_val.attribute9 country,
												-- oh_val.attribute10 addl_duty
							FROM apps.fnd_flex_value_sets oh_set,
												apps.fnd_flex_values_vl oh_val
						WHERE 1 = 1
						  AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
								AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
								AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
								AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
								AND oh_val.enabled_flag = 'Y'
								AND oh_val.attribute1 = pv_region
								AND NVL (oh_val.attribute8, pv_inv_org_id) = pv_inv_org_id
								AND NVL (oh_val.attribute9, pv_country) = pv_country;
								-- AND NVL (oh_val.attribute11, UPPER (pv_department)) = UPPER (pv_department)
								-- AND NVL (oh_val.attribute12, pv_style_number) = pv_style_number;

			CURSOR cur_comm_items (pv_style_number VARCHAR2, 
			                       pv_brand VARCHAR2, 
																										pv_inv_org_id VARCHAR2,
																										pv_department VARCHAR2)
			IS
					SELECT inventory_item_id inv_item_id,
												organization_id org_id,
												item_number item_number,
												style_number style_number,
												color_code style_color,
												item_size item_size
							FROM apps.xxd_common_items_v
						WHERE 1 = 1
						  AND style_number = pv_style_number
								AND brand = pv_brand
								AND organization_id = pv_inv_org_id
								AND UPPER (department) = UPPER (pv_department);

			l_group_id               NUMBER := xxdo.xxd_cst_duty_ele_upld_stg_t_s.NEXTVAL;
			lv_style_nuber           VARCHAR2(100);
			xv_errbuf                VARCHAR2 (4000);
			xv_retcode               VARCHAR2 (100);
			lv_tot_addl_duty_per     VARCHAR2 (240);
			lv_duty_rate             VARCHAR2 (240);
			lv_start_date            VARCHAR2 (240);
			lv_end_date              VARCHAR2 (240);
			lv_sku_weight_uom_code   VARCHAR2 (240);
		 lv_sku_unit_weight       VARCHAR2 (240);
			ln_ele_rec_success       NUMBER;
			ln_ele_rec_error         NUMBER;
			ln_ele_rec_total         NUMBER;
			ln_cnt                   NUMBER;
			ln_oh_cnt                NUMBER;
			ln_common_cnt            NUMBER;
			lv_duty                  VARCHAR2(1000);
			lv_operating_unit        VARCHAR2(1000);
			lv_organization_code     VARCHAR2(1000);
	  lv_oh_region             VARCHAR2(1000);
			lv_oh_brand              VARCHAR2(1000);
			lv_oh_duty               VARCHAR2(1000);
			lv_oh_freight            VARCHAR2(1000);
			lv_oh_oh_duty            VARCHAR2(1000);
			lv_oh_nonduty            VARCHAR2(1000);
			lv_oh_freight_duty       VARCHAR2(1000);
			lv_oh_inv_org_id         VARCHAR2(1000);
			lv_oh_country            VARCHAR2(1000);
			lv_oh_addl_duty          VARCHAR2(1000);
			lv_err_msg               VARCHAR2(1000);

		BEGIN
		     write_log_prc ('Procedure insert_into_custom_table_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

							BEGIN
												FOR i in cur_style_data
												LOOP
																BEGIN
																					SELECT style_number
																							INTO lv_style_nuber
																							FROM apps.xxd_common_items_v
																						WHERE  1 = 1
																								AND style_number = i.style_number
																								-- AND NVL (TRUNC (start_date_active), TRUNC (SYSDATE)) <= TRUNC (SYSDATE)
																								-- AND NVL (TRUNC (end_date_active), TRUNC (SYSDATE)) >= TRUNC (SYSDATE)
																								-- AND enabled_flag = 'Y'
																								AND ROWNUM = 1;

																					IF lv_style_nuber IS NOT NULL
																					THEN
																					    BEGIN
																														UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t
																																	SET rec_status = 'V'
																																				,error_msg = ''
																																				,request_id = gn_request_id
																															WHERE style_number = i.style_number
																															  AND country_of_origin = i.country_of_origin
																																	AND destination_country = i.destination_country
																																	AND rec_status = 'E'
																																	AND active_flag = 'Y'
																																	AND error_msg = 'Style Number is not Valid-'
																																	AND request_id <> gn_request_id;
																									EXCEPTION
																														WHEN OTHERS
																														THEN
																																		write_log_prc ('Exception Occurred while updating the rec status to V for the style number-'||i.style_number);
																										END;
																					END IF;

																					BEGIN
																										UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t
																													SET rec_status = 'V'
																																,error_msg = ''
																																,request_id = gn_request_id
																											WHERE style_number = i.style_number
																													AND country_of_origin = i.country_of_origin
																													AND destination_country = i.destination_country
																													AND rec_status = 'E'
																													AND active_flag = 'Y'
																													AND error_msg IN ('Unable To Derive OH Values From Value Set', 'Duty Element cannot be NULL')
																													AND request_id <> gn_request_id;
																					EXCEPTION
																										WHEN OTHERS
																										THEN
																														write_log_prc ('Exception Occurred while updating the rec status to V for OH Values-'||i.style_number);
																						END;

																EXCEPTION
																					WHEN OTHERS
																					THEN
																										write_log_prc ('Style Number'||SQLERRM);
																										lv_style_nuber := NULL;
																										BEGIN

																															UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t
																																		SET rec_status = 'E'
																																					,error_msg = 'Style Number is not Valid-'
																																					,request_id = gn_request_id
																																WHERE style_number = i.style_number
																																		AND country_of_origin = i.country_of_origin
																																		AND destination_country = i.destination_country
																																		AND rec_status = 'E'
																																		AND active_flag = 'Y'
																																		AND error_msg = 'Style Number is not Valid-'
																																		AND request_id <> gn_request_id;

																										EXCEPTION
																															WHEN OTHERS
																															THEN
																																			write_log_prc ('Exception Occurred while updating the rec status E for the style number-'||i.style_number);
																										END;

																END;

												END LOOP;

						 END;

							BEGIN
							     SELECT COUNT(1)
												  INTO ln_cnt
														FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t
													WHERE 1 = 1
															AND rec_status = 'V'
															AND active_flag = 'Y'
															AND error_msg IS NULL;

							EXCEPTION
							     WHEN OTHERS
												THEN
												    write_log_prc ('Exception occurred while retriving the count from xxdo.xxd_cst_duty_ele_inb_stg_tr_t');
												    ln_cnt := 0;
							END;

							IF ln_cnt > 0
							THEN

											FOR i in cur_tr_data
											LOOP

															write_log_prc ('style_number:'||i.style_number||	'country_of_origin:'||i.country_of_origin||'destination_country:'||i.destination_country||'hts_code:'||i.hts_code
																														||'duty_rate:'||i.duty_rate||'additonal_duty'||i.additional_duty||'applicable_uom'||i.applicable_uom||'preferential_rate:'||i.preferential_rate
																														||'pr_start_date'||i.pr_start_date||'pr_end_date:'||i.pr_end_date||'default_additional_duty'||i.default_additional_duty||'default_applicable_uom'
																														||i.default_applicable_uom||'effective_start_date:'||i.effective_start_date||'effective_end_date:'||i.effective_end_date||'active_flag:'||i.active_flag
																														||'preferential_duty_flag:'||i.preferential_duty_flag);

															FOR l in cur_derive_regn_cnty_org (i.country_of_origin, i.destination_country)
															LOOP

																			write_log_prc ('Region:'||l.region||'country code:'||l.country_code||'country:'||l.country||'inventory_org_id:'||l.inventory_org_id||'coo_precedence:'||l.coo_precedence);

																			BEGIN
																								lv_operating_unit:= NULL;
																								lv_organization_code := NULL;

																								SELECT operating_unit
																														,organization_code
																										INTO lv_operating_unit
																														,lv_organization_code
																										FROM apps.org_organization_definitions 
																									WHERE 1 = 1
																											AND organization_id = l.inventory_org_id;

																								--write_log_prc ('Derived Operating Unit:'||lv_operating_unit||'Organization_Code'||lv_organization_code||'Based on Invetory Org :'||l.inventory_org_id);	

																			EXCEPTION
																								WHEN OTHERS 
																								THEN
																												write_log_prc (SQLERRM||' Failed to  Retrive Operating Unit for the Inv Org-'||l.inventory_org_id			);
																			END;

																		 -- FOR j IN cur_oh_ele_data (l.region, l.country_code, l.inventory_org_id)
																			FOR j IN cur_oh_ele_data (l.region, l.country, l.inventory_org_id)	-- Modified per CCR0010355
																			LOOP
																							-- write_log_prc ('brand'||j.brand);


																							BEGIN
																												lv_oh_region := NULL;
																												lv_oh_brand := NULL;
																												lv_oh_duty  := NULL;
																												lv_oh_freight := NULL;
																												lv_oh_oh_duty := NULL;
																												lv_oh_nonduty := NULL;
																												lv_oh_freight_duty := NULL;
																												lv_oh_inv_org_id := NULL;
																												lv_oh_country := NULL;
																												lv_oh_addl_duty := NULL;

																												SELECT oh_val.attribute1 region,
																																			oh_val.attribute2 brand, 
																																			oh_val.attribute3 duty,
																																			oh_val.attribute4 freight,
																																			oh_val.attribute5 oh_duty,
																																			oh_val.attribute6 oh_nonduty,
																																			oh_val.attribute7 freight_duty,
																																			oh_val.attribute8 inv_org_id,
																																			oh_val.attribute9 country,
																																			oh_val.attribute10 addl_duty
																														INTO lv_oh_region,
																																			lv_oh_brand,
																																			lv_oh_duty,
																																			lv_oh_freight,
																																			lv_oh_oh_duty,
																																			lv_oh_nonduty,
																																			lv_oh_freight_duty,
																																			lv_oh_inv_org_id,
																																			lv_oh_country,
																																			lv_oh_addl_duty
																														FROM apps.fnd_flex_value_sets oh_set,
																																			apps.fnd_flex_values_vl oh_val
																													WHERE 1 = 1
																															AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																															AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																															AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																															AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																															AND oh_val.enabled_flag = 'Y'
																															AND oh_val.attribute1 = l.region
																															AND oh_val.attribute2 = j.brand
																															AND oh_val.attribute8 = l.inventory_org_id
																															AND oh_val.attribute9 = l.country_code
																															AND UPPER (oh_val.attribute11) = UPPER (i.department)
																															AND oh_val.attribute12 = i.style_number;

																							EXCEPTION
																												WHEN NO_DATA_FOUND
																												THEN
																												    -- write_log_prc ('No OHE For Country, Org, Department, Style Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																lv_oh_region := NULL;
																																lv_oh_brand := NULL;
																																lv_oh_duty  := NULL;
																																lv_oh_freight := NULL;
																																lv_oh_oh_duty := NULL;
																																lv_oh_nonduty := NULL;
																																lv_oh_freight_duty := NULL;
																																lv_oh_inv_org_id := NULL;
																																lv_oh_country := NULL;
																																lv_oh_addl_duty := NULL;

																																BEGIN																																			
																																					SELECT oh_val.attribute1 region,
																																												oh_val.attribute2 brand, 
																																												oh_val.attribute3 duty,
																																												oh_val.attribute4 freight,
																																												oh_val.attribute5 oh_duty,
																																												oh_val.attribute6 oh_nonduty,
																																												oh_val.attribute7 freight_duty,
																																												oh_val.attribute8 inv_org_id,
																																												oh_val.attribute9 country,
																																												oh_val.attribute10 addl_duty
																																							INTO lv_oh_region,
																																												lv_oh_brand,
																																												lv_oh_duty,
																																												lv_oh_freight,
																																												lv_oh_oh_duty,
																																												lv_oh_nonduty,
																																												lv_oh_freight_duty,
																																												lv_oh_inv_org_id,
																																												lv_oh_country,
																																												lv_oh_addl_duty
																																							FROM apps.fnd_flex_value_sets oh_set,
																																												apps.fnd_flex_values_vl oh_val
																																						WHERE 1 = 1
																																								AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																								AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																								AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																								AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																								AND oh_val.enabled_flag = 'Y'
																																								AND oh_val.attribute1 = l.region
																																								AND oh_val.attribute2 = j.brand
																																								AND oh_val.attribute8 = l.inventory_org_id
																																								AND oh_val.attribute9 IS NULL
																																								AND UPPER (oh_val.attribute11) = UPPER (i.department)
																																								AND oh_val.attribute12 = i.style_number;

																																EXCEPTION
																																					WHEN NO_DATA_FOUND
																																					THEN
																																					    -- write_log_prc ('No OHE For Country Is Null, Org, Department, Style Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																									lv_oh_region := NULL;
																																									lv_oh_brand := NULL;
																																									lv_oh_duty  := NULL;
																																									lv_oh_freight := NULL;
																																									lv_oh_oh_duty := NULL;
																																									lv_oh_nonduty := NULL;
																																									lv_oh_freight_duty := NULL;
																																									lv_oh_inv_org_id := NULL;
																																									lv_oh_country := NULL;
																																									lv_oh_addl_duty := NULL;

																																									BEGIN
																																														SELECT oh_val.attribute1 region,
																																																					oh_val.attribute2 brand, 
																																																					oh_val.attribute3 duty,
																																																					oh_val.attribute4 freight,
																																																					oh_val.attribute5 oh_duty,
																																																					oh_val.attribute6 oh_nonduty,
																																																					oh_val.attribute7 freight_duty,
																																																					oh_val.attribute8 inv_org_id,
																																																					oh_val.attribute9 country,
																																																					oh_val.attribute10 addl_duty
																																																INTO lv_oh_region,
																																																					lv_oh_brand,
																																																					lv_oh_duty,
																																																					lv_oh_freight,
																																																					lv_oh_oh_duty,
																																																					lv_oh_nonduty,
																																																					lv_oh_freight_duty,
																																																					lv_oh_inv_org_id,
																																																					lv_oh_country,
																																																					lv_oh_addl_duty
																																																FROM apps.fnd_flex_value_sets oh_set,
																																																					apps.fnd_flex_values_vl oh_val
																																															WHERE 1 = 1
																																																	AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																	AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																	AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																	AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																	AND oh_val.enabled_flag = 'Y'
																																																	AND oh_val.attribute1 = l.region
																																																	AND oh_val.attribute2 = j.brand
																																																	AND oh_val.attribute8 IS NULL
																																																	AND oh_val.attribute9 = l.country_code
																																																	AND UPPER (oh_val.attribute11) = UPPER (i.department)
																																																	AND oh_val.attribute12 = i.style_number;

																																									EXCEPTION
																																														WHEN NO_DATA_FOUND
																																														THEN
																																														    -- write_log_prc ('No OHE For Country, Department, Style Not Null, Org Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																		lv_oh_region := NULL;
																																																		lv_oh_brand := NULL;
																																																		lv_oh_duty  := NULL;
																																																		lv_oh_freight := NULL;
																																																		lv_oh_oh_duty := NULL;
																																																		lv_oh_nonduty := NULL;
																																																		lv_oh_freight_duty := NULL;
																																																		lv_oh_inv_org_id := NULL;
																																																		lv_oh_country := NULL;
																																																		lv_oh_addl_duty := NULL;

																																																		BEGIN
																																																							SELECT oh_val.attribute1 region,
																																																														oh_val.attribute2 brand, 
																																																														oh_val.attribute3 duty,
																																																														oh_val.attribute4 freight,
																																																														oh_val.attribute5 oh_duty,
																																																														oh_val.attribute6 oh_nonduty,
																																																														oh_val.attribute7 freight_duty,
																																																														oh_val.attribute8 inv_org_id,
																																																														oh_val.attribute9 country,
																																																														oh_val.attribute10 addl_duty
																																																									INTO lv_oh_region,
																																																														lv_oh_brand,
																																																														lv_oh_duty,
																																																														lv_oh_freight,
																																																														lv_oh_oh_duty,
																																																														lv_oh_nonduty,
																																																														lv_oh_freight_duty,
																																																														lv_oh_inv_org_id,
																																																														lv_oh_country,
																																																														lv_oh_addl_duty
																																																									FROM apps.fnd_flex_value_sets oh_set,
																																																														apps.fnd_flex_values_vl oh_val
																																																								WHERE 1 = 1
																																																										AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																										AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																										AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																										AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																										AND oh_val.enabled_flag = 'Y'
																																																										AND oh_val.attribute1 = l.region
																																																										AND oh_val.attribute2 = j.brand
																																																										AND oh_val.attribute8 IS NULL
																																																										AND oh_val.attribute9 IS NULL
																																																										AND UPPER (oh_val.attribute11) = UPPER (i.department)
																																																										AND oh_val.attribute12 = i.style_number;

																																																		EXCEPTION
																																																							WHEN NO_DATA_FOUND
																																																							THEN
																																																											-- write_log_prc ('No OHE For Country, Org Null, Department, Style Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																											lv_oh_region := NULL;
																																																											lv_oh_brand := NULL;
																																																											lv_oh_duty  := NULL;
																																																											lv_oh_freight := NULL;
																																																											lv_oh_oh_duty := NULL;
																																																											lv_oh_nonduty := NULL;
																																																											lv_oh_freight_duty := NULL;
																																																											lv_oh_inv_org_id := NULL;
																																																											lv_oh_country := NULL;
																																																											lv_oh_addl_duty := NULL;

																																																											BEGIN
																																																																SELECT oh_val.attribute1 region,
																																																																							oh_val.attribute2 brand, 
																																																																							oh_val.attribute3 duty,
																																																																							oh_val.attribute4 freight,
																																																																							oh_val.attribute5 oh_duty,
																																																																							oh_val.attribute6 oh_nonduty,
																																																																							oh_val.attribute7 freight_duty,
																																																																							oh_val.attribute8 inv_org_id,
																																																																							oh_val.attribute9 country,
																																																																							oh_val.attribute10 addl_duty
																																																																		INTO lv_oh_region,
																																																																							lv_oh_brand,
																																																																							lv_oh_duty,
																																																																							lv_oh_freight,
																																																																							lv_oh_oh_duty,
																																																																							lv_oh_nonduty,
																																																																							lv_oh_freight_duty,
																																																																							lv_oh_inv_org_id,
																																																																							lv_oh_country,
																																																																							lv_oh_addl_duty
																																																																		FROM apps.fnd_flex_value_sets oh_set,
																																																																							apps.fnd_flex_values_vl oh_val
																																																																	WHERE 1 = 1
																																																																			AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																			AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																			AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																			AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																			AND oh_val.enabled_flag = 'Y'
																																																																			AND oh_val.attribute1 = l.region
																																																																			AND oh_val.attribute2 = j.brand
																																																																			AND oh_val.attribute8 = l.inventory_org_id
																																																																			AND oh_val.attribute9 = l.country_code
																																																																			AND oh_val.attribute11 IS NULL
																																																																			AND oh_val.attribute12 = i.style_number;

																																																											EXCEPTION
																																																																WHEN NO_DATA_FOUND
																																																																THEN
																																																																				-- write_log_prc ('No OHE For Country, Org, Style Not Null, Department Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																				lv_oh_region := NULL;
																																																																				lv_oh_brand := NULL;
																																																																				lv_oh_duty  := NULL;
																																																																				lv_oh_freight := NULL;
																																																																				lv_oh_oh_duty := NULL;
																																																																				lv_oh_nonduty := NULL;
																																																																				lv_oh_freight_duty := NULL;
																																																																				lv_oh_inv_org_id := NULL;
																																																																				lv_oh_country := NULL;
																																																																				lv_oh_addl_duty := NULL;

																																																																				BEGIN
																																																																									SELECT oh_val.attribute1 region,
																																																																																oh_val.attribute2 brand, 
																																																																																oh_val.attribute3 duty,
																																																																																oh_val.attribute4 freight,
																																																																																oh_val.attribute5 oh_duty,
																																																																																oh_val.attribute6 oh_nonduty,
																																																																																oh_val.attribute7 freight_duty,
																																																																																oh_val.attribute8 inv_org_id,
																																																																																oh_val.attribute9 country,
																																																																																oh_val.attribute10 addl_duty
																																																																											INTO lv_oh_region,
																																																																																lv_oh_brand,
																																																																																lv_oh_duty,
																																																																																lv_oh_freight,
																																																																																lv_oh_oh_duty,
																																																																																lv_oh_nonduty,
																																																																																lv_oh_freight_duty,
																																																																																lv_oh_inv_org_id,
																																																																																lv_oh_country,
																																																																																lv_oh_addl_duty
																																																																											FROM apps.fnd_flex_value_sets oh_set,
																																																																																apps.fnd_flex_values_vl oh_val
																																																																										WHERE 1 = 1
																																																																												AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																												AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																												AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																												AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																												AND oh_val.enabled_flag = 'Y'
																																																																												AND oh_val.attribute1 = l.region
																																																																												AND oh_val.attribute2 = j.brand
																																																																												AND oh_val.attribute8 = l.inventory_org_id
																																																																												AND oh_val.attribute9 IS NULL
																																																																												AND oh_val.attribute11 IS NULL
																																																																												AND oh_val.attribute12 = i.style_number;

																																																																				EXCEPTION
																																																																									WHEN NO_DATA_FOUND
																																																																									THEN
                                                                             -- write_log_prc ('No OHE For Country, Department Null, Org, Style Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																													lv_oh_region := NULL;
																																																																													lv_oh_brand := NULL;
																																																																													lv_oh_duty  := NULL;
																																																																													lv_oh_freight := NULL;
																																																																													lv_oh_oh_duty := NULL;
																																																																													lv_oh_nonduty := NULL;
																																																																													lv_oh_freight_duty := NULL;
																																																																													lv_oh_inv_org_id := NULL;
																																																																													lv_oh_country := NULL;
																																																																													lv_oh_addl_duty := NULL;

																																																																													BEGIN
																																																																																		SELECT oh_val.attribute1 region,
																																																																																									oh_val.attribute2 brand, 
																																																																																									oh_val.attribute3 duty,
																																																																																									oh_val.attribute4 freight,
																																																																																									oh_val.attribute5 oh_duty,
																																																																																									oh_val.attribute6 oh_nonduty,
																																																																																									oh_val.attribute7 freight_duty,
																																																																																									oh_val.attribute8 inv_org_id,
																																																																																									oh_val.attribute9 country,
																																																																																									oh_val.attribute10 addl_duty
																																																																																				INTO lv_oh_region,
																																																																																									lv_oh_brand,
																																																																																									lv_oh_duty,
																																																																																									lv_oh_freight,
																																																																																									lv_oh_oh_duty,
																																																																																									lv_oh_nonduty,
																																																																																									lv_oh_freight_duty,
																																																																																									lv_oh_inv_org_id,
																																																																																									lv_oh_country,
																																																																																									lv_oh_addl_duty
																																																																																				FROM apps.fnd_flex_value_sets oh_set,
																																																																																									apps.fnd_flex_values_vl oh_val
																																																																																			WHERE 1 = 1
																																																																																					AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																																					AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																																					AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																																					AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																																					AND oh_val.enabled_flag = 'Y'
																																																																																					AND oh_val.attribute1 = l.region
																																																																																					AND oh_val.attribute2 = j.brand
																																																																																					AND oh_val.attribute8 IS NULL
																																																																																					AND oh_val.attribute9 = l.country_code
																																																																																					AND oh_val.attribute11 IS NULL
																																																																																					AND oh_val.attribute12 = i.style_number;

																																																																													EXCEPTION
																																																																																		WHEN NO_DATA_FOUND
																																																																																		THEN
																																																																																						-- write_log_prc ('No OHE For Country, Style Not Null, Org, Department Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																						lv_oh_region := NULL;
																																																																																						lv_oh_brand := NULL;
																																																																																						lv_oh_duty  := NULL;
																																																																																						lv_oh_freight := NULL;
																																																																																						lv_oh_oh_duty := NULL;
																																																																																						lv_oh_nonduty := NULL;
																																																																																						lv_oh_freight_duty := NULL;
																																																																																						lv_oh_inv_org_id := NULL;
																																																																																						lv_oh_country := NULL;
																																																																																						lv_oh_addl_duty := NULL;

																																																																																						BEGIN
																																																																																											SELECT oh_val.attribute1 region,
																																																																																																		oh_val.attribute2 brand, 
																																																																																																		oh_val.attribute3 duty,
																																																																																																		oh_val.attribute4 freight,
																																																																																																		oh_val.attribute5 oh_duty,
																																																																																																		oh_val.attribute6 oh_nonduty,
																																																																																																		oh_val.attribute7 freight_duty,
																																																																																																		oh_val.attribute8 inv_org_id,
																																																																																																		oh_val.attribute9 country,
																																																																																																		oh_val.attribute10 addl_duty
																																																																																													INTO lv_oh_region,
																																																																																																		lv_oh_brand,
																																																																																																		lv_oh_duty,
																																																																																																		lv_oh_freight,
																																																																																																		lv_oh_oh_duty,
																																																																																																		lv_oh_nonduty,
																																																																																																		lv_oh_freight_duty,
																																																																																																		lv_oh_inv_org_id,
																																																																																																		lv_oh_country,
																																																																																																		lv_oh_addl_duty
																																																																																													FROM apps.fnd_flex_value_sets oh_set,
																																																																																																		apps.fnd_flex_values_vl oh_val
																																																																																												WHERE 1 = 1
																																																																																														AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																																														AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																																														AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																																														AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																																														AND oh_val.enabled_flag = 'Y'
																																																																																														AND oh_val.attribute1 = l.region
																																																																																														AND oh_val.attribute2 = j.brand
																																																																																														AND oh_val.attribute8 IS NULL
																																																																																														AND oh_val.attribute9 IS NULL
																																																																																														AND oh_val.attribute11 IS NULL
																																																																																														AND oh_val.attribute12 = i.style_number;

																																																																																						EXCEPTION
																																																																																											WHEN NO_DATA_FOUND
																																																																																											THEN
																																																																																															-- write_log_prc ('No OHE For Country, Org, Department Null, Style Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																															lv_oh_region := NULL;
																																																																																															lv_oh_brand := NULL;
																																																																																															lv_oh_duty  := NULL;
																																																																																															lv_oh_freight := NULL;
																																																																																															lv_oh_oh_duty := NULL;
																																																																																															lv_oh_nonduty := NULL;
																																																																																															lv_oh_freight_duty := NULL;
																																																																																															lv_oh_inv_org_id := NULL;
																																																																																															lv_oh_country := NULL;
																																																																																															lv_oh_addl_duty := NULL;

																																																																																															BEGIN
																																																																																																				SELECT oh_val.attribute1 region,
																																																																																																											oh_val.attribute2 brand, 
																																																																																																											oh_val.attribute3 duty,
																																																																																																											oh_val.attribute4 freight,
																																																																																																											oh_val.attribute5 oh_duty,
																																																																																																											oh_val.attribute6 oh_nonduty,
																																																																																																											oh_val.attribute7 freight_duty,
																																																																																																											oh_val.attribute8 inv_org_id,
																																																																																																											oh_val.attribute9 country,
																																																																																																											oh_val.attribute10 addl_duty
																																																																																																						INTO lv_oh_region,
																																																																																																											lv_oh_brand,
																																																																																																											lv_oh_duty,
																																																																																																											lv_oh_freight,
																																																																																																											lv_oh_oh_duty,
																																																																																																											lv_oh_nonduty,
																																																																																																											lv_oh_freight_duty,
																																																																																																											lv_oh_inv_org_id,
																																																																																																											lv_oh_country,
																																																																																																											lv_oh_addl_duty
																																																																																																						FROM apps.fnd_flex_value_sets oh_set,
																																																																																																											apps.fnd_flex_values_vl oh_val
																																																																																																					WHERE 1 = 1
																																																																																																							AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																																																							AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																																																							AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																																																							AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																																																							AND oh_val.enabled_flag = 'Y'
																																																																																																							AND oh_val.attribute1 = l.region
																																																																																																							AND oh_val.attribute2 = j.brand
																																																																																																							AND oh_val.attribute8 = l.inventory_org_id
																																																																																																							AND oh_val.attribute9 = l.country_code
																																																																																																							AND UPPER (oh_val.attribute11) = UPPER (i.department)
																																																																																																							AND oh_val.attribute12 IS NULL;

																																																																																															EXCEPTION
																																																																																																				WHEN NO_DATA_FOUND
																																																																																																				THEN
																																																																																																								-- write_log_prc ('No OHE For Country, Org, Department Not Null, Style Null,'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																																								lv_oh_region := NULL;
																																																																																																								lv_oh_brand := NULL;
																																																																																																								lv_oh_duty  := NULL;
																																																																																																								lv_oh_freight := NULL;
																																																																																																								lv_oh_oh_duty := NULL;
																																																																																																								lv_oh_nonduty := NULL;
																																																																																																								lv_oh_freight_duty := NULL;
																																																																																																								lv_oh_inv_org_id := NULL;
																																																																																																								lv_oh_country := NULL;
																																																																																																								lv_oh_addl_duty := NULL;

																																																																																																								BEGIN
																																																																																																													SELECT oh_val.attribute1 region,
																																																																																																																				oh_val.attribute2 brand, 
																																																																																																																				oh_val.attribute3 duty,
																																																																																																																				oh_val.attribute4 freight,
																																																																																																																				oh_val.attribute5 oh_duty,
																																																																																																																				oh_val.attribute6 oh_nonduty,
																																																																																																																				oh_val.attribute7 freight_duty,
																																																																																																																				oh_val.attribute8 inv_org_id,
																																																																																																																				oh_val.attribute9 country,
																																																																																																																				oh_val.attribute10 addl_duty
																																																																																																															INTO lv_oh_region,
																																																																																																																				lv_oh_brand,
																																																																																																																				lv_oh_duty,
																																																																																																																				lv_oh_freight,
																																																																																																																				lv_oh_oh_duty,
																																																																																																																				lv_oh_nonduty,
																																																																																																																				lv_oh_freight_duty,
																																																																																																																				lv_oh_inv_org_id,
																																																																																																																				lv_oh_country,
																																																																																																																				lv_oh_addl_duty
																																																																																																															FROM apps.fnd_flex_value_sets oh_set,
																																																																																																																				apps.fnd_flex_values_vl oh_val
																																																																																																														WHERE 1 = 1
																																																																																																																AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																																																																AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																																																																AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																																																																AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																																																																AND oh_val.enabled_flag = 'Y'
																																																																																																																AND oh_val.attribute1 = l.region
																																																																																																																AND oh_val.attribute2 = j.brand
																																																																																																																AND oh_val.attribute8 = l.inventory_org_id
																																																																																																																AND oh_val.attribute9 IS NULL
																																																																																																																AND UPPER (oh_val.attribute11) = UPPER (i.department)
																																																																																																																AND oh_val.attribute12 IS NULL;

																																																																																																								EXCEPTION
																																																																																																													WHEN NO_DATA_FOUND
																																																																																																													THEN
																																																																																																													    -- write_log_prc ('No OHE For Country, Style Null, Org, Department Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																																																	lv_oh_region := NULL;
																																																																																																																	lv_oh_brand := NULL;
																																																																																																																	lv_oh_duty  := NULL;
																																																																																																																	lv_oh_freight := NULL;
																																																																																																																	lv_oh_oh_duty := NULL;
																																																																																																																	lv_oh_nonduty := NULL;
																																																																																																																	lv_oh_freight_duty := NULL;
																																																																																																																	lv_oh_inv_org_id := NULL;
																																																																																																																	lv_oh_country := NULL;
																																																																																																																	lv_oh_addl_duty := NULL;

																																																																																																																	BEGIN
																																																																																																																						SELECT oh_val.attribute1 region,
																																																																																																																													oh_val.attribute2 brand, 
																																																																																																																													oh_val.attribute3 duty,
																																																																																																																													oh_val.attribute4 freight,
																																																																																																																													oh_val.attribute5 oh_duty,
																																																																																																																													oh_val.attribute6 oh_nonduty,
																																																																																																																													oh_val.attribute7 freight_duty,
																																																																																																																													oh_val.attribute8 inv_org_id,
																																																																																																																													oh_val.attribute9 country,
																																																																																																																													oh_val.attribute10 addl_duty
																																																																																																																								INTO lv_oh_region,
																																																																																																																													lv_oh_brand,
																																																																																																																													lv_oh_duty,
																																																																																																																													lv_oh_freight,
																																																																																																																													lv_oh_oh_duty,
																																																																																																																													lv_oh_nonduty,
																																																																																																																													lv_oh_freight_duty,
																																																																																																																													lv_oh_inv_org_id,
																																																																																																																													lv_oh_country,
																																																																																																																													lv_oh_addl_duty
																																																																																																																								FROM apps.fnd_flex_value_sets oh_set,
																																																																																																																													apps.fnd_flex_values_vl oh_val
																																																																																																																							WHERE 1 = 1
																																																																																																																									AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																																																																									AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																																																																									AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																																																																									AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																																																																									AND oh_val.enabled_flag = 'Y'
																																																																																																																									AND oh_val.attribute1 = l.region
																																																																																																																									AND oh_val.attribute2 = j.brand
																																																																																																																									AND oh_val.attribute8 IS NULL
																																																																																																																									AND oh_val.attribute9 = l.country_code
																																																																																																																									AND UPPER (oh_val.attribute11) = UPPER (i.department)
																																																																																																																									AND oh_val.attribute12 IS NULL;

																																																																																																																	EXCEPTION
																																																																																																																						WHEN NO_DATA_FOUND
																																																																																																																						THEN
																																																																																																																										-- write_log_prc ('No OHE For Country, Department Not Null, Org, Style Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																																																										lv_oh_region := NULL;
																																																																																																																										lv_oh_brand := NULL;
																																																																																																																										lv_oh_duty  := NULL;
																																																																																																																										lv_oh_freight := NULL;
																																																																																																																										lv_oh_oh_duty := NULL;
																																																																																																																										lv_oh_nonduty := NULL;
																																																																																																																										lv_oh_freight_duty := NULL;
																																																																																																																										lv_oh_inv_org_id := NULL;
																																																																																																																										lv_oh_country := NULL;
																																																																																																																										lv_oh_addl_duty := NULL;

																																																																																																																										BEGIN
																																																																																																																															SELECT oh_val.attribute1 region,
																																																																																																																																						oh_val.attribute2 brand, 
																																																																																																																																						oh_val.attribute3 duty,
																																																																																																																																						oh_val.attribute4 freight,
																																																																																																																																						oh_val.attribute5 oh_duty,
																																																																																																																																						oh_val.attribute6 oh_nonduty,
																																																																																																																																						oh_val.attribute7 freight_duty,
																																																																																																																																						oh_val.attribute8 inv_org_id,
																																																																																																																																						oh_val.attribute9 country,
																																																																																																																																						oh_val.attribute10 addl_duty
																																																																																																																																	INTO lv_oh_region,
																																																																																																																																						lv_oh_brand,
																																																																																																																																						lv_oh_duty,
																																																																																																																																						lv_oh_freight,
																																																																																																																																						lv_oh_oh_duty,
																																																																																																																																						lv_oh_nonduty,
																																																																																																																																						lv_oh_freight_duty,
																																																																																																																																						lv_oh_inv_org_id,
																																																																																																																																						lv_oh_country,
																																																																																																																																						lv_oh_addl_duty
																																																																																																																																	FROM apps.fnd_flex_value_sets oh_set,
																																																																																																																																						apps.fnd_flex_values_vl oh_val
																																																																																																																																WHERE 1 = 1
																																																																																																																																		AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																																																																																		AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																																																																																		AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																																																																																		AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																																																																																		AND oh_val.enabled_flag = 'Y'
																																																																																																																																		AND oh_val.attribute1 = l.region
																																																																																																																																		AND oh_val.attribute2 = j.brand
																																																																																																																																		AND oh_val.attribute8 IS NULL
																																																																																																																																		AND oh_val.attribute9 IS NULL
																																																																																																																																		AND UPPER (oh_val.attribute11) = UPPER (i.department)
																																																																																																																																		AND oh_val.attribute12 IS NULL;

																																																																																																																										EXCEPTION
																																																																																																																															WHEN NO_DATA_FOUND
																																																																																																																															THEN
																																																																																																																																			-- write_log_prc ('No OHE For Country, Org, Style Null, Department Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																																																																			lv_oh_region := NULL;
																																																																																																																																			lv_oh_brand := NULL;
																																																																																																																																			lv_oh_duty  := NULL;
																																																																																																																																			lv_oh_freight := NULL;
																																																																																																																																			lv_oh_oh_duty := NULL;
																																																																																																																																			lv_oh_nonduty := NULL;
																																																																																																																																			lv_oh_freight_duty := NULL;
																																																																																																																																			lv_oh_inv_org_id := NULL;
																																																																																																																																			lv_oh_country := NULL;
																																																																																																																																			lv_oh_addl_duty := NULL;

																																																																																																																																			BEGIN
																																																																																																																																								SELECT oh_val.attribute1 region,
																																																																																																																																															oh_val.attribute2 brand, 
																																																																																																																																															oh_val.attribute3 duty,
																																																																																																																																															oh_val.attribute4 freight,
																																																																																																																																															oh_val.attribute5 oh_duty,
																																																																																																																																															oh_val.attribute6 oh_nonduty,
																																																																																																																																															oh_val.attribute7 freight_duty,
																																																																																																																																															oh_val.attribute8 inv_org_id,
																																																																																																																																															oh_val.attribute9 country,
																																																																																																																																															oh_val.attribute10 addl_duty
																																																																																																																																										INTO lv_oh_region,
																																																																																																																																															lv_oh_brand,
																																																																																																																																															lv_oh_duty,
																																																																																																																																															lv_oh_freight,
																																																																																																																																															lv_oh_oh_duty,
																																																																																																																																															lv_oh_nonduty,
																																																																																																																																															lv_oh_freight_duty,
																																																																																																																																															lv_oh_inv_org_id,
																																																																																																																																															lv_oh_country,
																																																																																																																																															lv_oh_addl_duty
																																																																																																																																										FROM apps.fnd_flex_value_sets oh_set,
																																																																																																																																															apps.fnd_flex_values_vl oh_val
																																																																																																																																									WHERE 1 = 1
																																																																																																																																											AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																																																																																											AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																																																																																											AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																																																																																											AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																																																																																											AND oh_val.enabled_flag = 'Y'
																																																																																																																																											AND oh_val.attribute1 = l.region
																																																																																																																																											AND oh_val.attribute2 = j.brand
																																																																																																																																											AND oh_val.attribute8 = l.inventory_org_id
																																																																																																																																											AND oh_val.attribute9 = l.country_code
																																																																																																																																											AND oh_val.attribute11 IS NULL
																																																																																																																																											AND oh_val.attribute12 IS NULL;

																																																																																																																																			EXCEPTION
																																																																																																																																								WHEN NO_DATA_FOUND
																																																																																																																																								THEN
																																																																																																																																												-- write_log_prc ('No OHE For Country, Org Not Null, Department, Style Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																																																																												lv_oh_region := NULL;
																																																																																																																																												lv_oh_brand := NULL;
																																																																																																																																												lv_oh_duty  := NULL;
																																																																																																																																												lv_oh_freight := NULL;
																																																																																																																																												lv_oh_oh_duty := NULL;
																																																																																																																																												lv_oh_nonduty := NULL;
																																																																																																																																												lv_oh_freight_duty := NULL;
																																																																																																																																												lv_oh_inv_org_id := NULL;
																																																																																																																																												lv_oh_country := NULL;
																																																																																																																																												lv_oh_addl_duty := NULL;

																																																																																																																																												BEGIN
																																																																																																																																																	SELECT oh_val.attribute1 region,
																																																																																																																																																								oh_val.attribute2 brand, 
																																																																																																																																																								oh_val.attribute3 duty,
																																																																																																																																																								oh_val.attribute4 freight,
																																																																																																																																																								oh_val.attribute5 oh_duty,
																																																																																																																																																								oh_val.attribute6 oh_nonduty,
																																																																																																																																																								oh_val.attribute7 freight_duty,
																																																																																																																																																								oh_val.attribute8 inv_org_id,
																																																																																																																																																								oh_val.attribute9 country,
																																																																																																																																																								oh_val.attribute10 addl_duty
																																																																																																																																																			INTO lv_oh_region,
																																																																																																																																																								lv_oh_brand,
																																																																																																																																																								lv_oh_duty,
																																																																																																																																																								lv_oh_freight,
																																																																																																																																																								lv_oh_oh_duty,
																																																																																																																																																								lv_oh_nonduty,
																																																																																																																																																								lv_oh_freight_duty,
																																																																																																																																																								lv_oh_inv_org_id,
																																																																																																																																																								lv_oh_country,
																																																																																																																																																								lv_oh_addl_duty
																																																																																																																																																			FROM apps.fnd_flex_value_sets oh_set,
																																																																																																																																																								apps.fnd_flex_values_vl oh_val
																																																																																																																																																		WHERE 1 = 1
																																																																																																																																																				AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																																																																																																				AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																																																																																																				AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																																																																																																				AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																																																																																																				AND oh_val.enabled_flag = 'Y'
																																																																																																																																																				AND oh_val.attribute1 = l.region
																																																																																																																																																				AND oh_val.attribute2 = j.brand
																																																																																																																																																				AND oh_val.attribute8 = l.inventory_org_id
																																																																																																																																																				AND oh_val.attribute9 IS NULL
																																																																																																																																																				AND oh_val.attribute11 IS NULL
																																																																																																																																																				AND oh_val.attribute12 IS NULL;

																																																																																																																																												EXCEPTION
																																																																																																																																																	WHEN NO_DATA_FOUND
																																																																																																																																																	THEN
                                                                                                                                                     -- write_log_prc ('No OHE For Country, Department, Style Is Null, Org Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																																																																																					lv_oh_region := NULL;
																																																																																																																																																					lv_oh_brand := NULL;
																																																																																																																																																					lv_oh_duty  := NULL;
																																																																																																																																																					lv_oh_freight := NULL;
																																																																																																																																																					lv_oh_oh_duty := NULL;
																																																																																																																																																					lv_oh_nonduty := NULL;
																																																																																																																																																					lv_oh_freight_duty := NULL;
																																																																																																																																																					lv_oh_inv_org_id := NULL;
																																																																																																																																																					lv_oh_country := NULL;
																																																																																																																																																					lv_oh_addl_duty := NULL;

																																																																																																																																																					BEGIN
																																																																																																																																																										SELECT oh_val.attribute1 region,
																																																																																																																																																																	oh_val.attribute2 brand, 
																																																																																																																																																																	oh_val.attribute3 duty,
																																																																																																																																																																	oh_val.attribute4 freight,
																																																																																																																																																																	oh_val.attribute5 oh_duty,
																																																																																																																																																																	oh_val.attribute6 oh_nonduty,
																																																																																																																																																																	oh_val.attribute7 freight_duty,
																																																																																																																																																																	oh_val.attribute8 inv_org_id,
																																																																																																																																																																	oh_val.attribute9 country,
																																																																																																																																																																	oh_val.attribute10 addl_duty
																																																																																																																																																												INTO lv_oh_region,
																																																																																																																																																																	lv_oh_brand,
																																																																																																																																																																	lv_oh_duty,
																																																																																																																																																																	lv_oh_freight,
																																																																																																																																																																	lv_oh_oh_duty,
																																																																																																																																																																	lv_oh_nonduty,
																																																																																																																																																																	lv_oh_freight_duty,
																																																																																																																																																																	lv_oh_inv_org_id,
																																																																																																																																																																	lv_oh_country,
																																																																																																																																																																	lv_oh_addl_duty
																																																																																																																																																												FROM apps.fnd_flex_value_sets oh_set,
																																																																																																																																																																	apps.fnd_flex_values_vl oh_val
																																																																																																																																																											WHERE 1 = 1
																																																																																																																																																													AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																																																																																																													AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																																																																																																													AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																																																																																																													AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																																																																																																													AND oh_val.enabled_flag = 'Y'
																																																																																																																																																													AND oh_val.attribute1 = l.region
																																																																																																																																																													AND oh_val.attribute2 = j.brand
																																																																																																																																																													AND oh_val.attribute8 IS NULL
																																																																																																																																																													AND oh_val.attribute9 = l.country_code
																																																																																																																																																													AND oh_val.attribute11 IS NULL
																																																																																																																																																													AND oh_val.attribute12 IS NULL;

																																																																																																																																																					EXCEPTION
																																																																																																																																																										WHEN NO_DATA_FOUND
																																																																																																																																																										THEN
																																																																																																																																																														-- write_log_prc ('No OHE For Country Not Null, Org, Department, Style Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																																																																																														lv_oh_region := NULL;
																																																																																																																																																														lv_oh_brand := NULL;
																																																																																																																																																														lv_oh_duty  := NULL;
																																																																																																																																																														lv_oh_freight := NULL;
																																																																																																																																																														lv_oh_oh_duty := NULL;
																																																																																																																																																														lv_oh_nonduty := NULL;
																																																																																																																																																														lv_oh_freight_duty := NULL;
																																																																																																																																																														lv_oh_inv_org_id := NULL;
																																																																																																																																																														lv_oh_country := NULL;
																																																																																																																																																														lv_oh_addl_duty := NULL;
																																																																																																																																																														BEGIN
																																																																																																																																																																			SELECT oh_val.attribute1 region,
																																																																																																																																																																										oh_val.attribute2 brand, 
																																																																																																																																																																										oh_val.attribute3 duty,
																																																																																																																																																																										oh_val.attribute4 freight,
																																																																																																																																																																										oh_val.attribute5 oh_duty,
																																																																																																																																																																										oh_val.attribute6 oh_nonduty,
																																																																																																																																																																										oh_val.attribute7 freight_duty,
																																																																																																																																																																										oh_val.attribute8 inv_org_id,
																																																																																																																																																																										oh_val.attribute9 country,
																																																																																																																																																																										oh_val.attribute10 addl_duty
																																																																																																																																																																					INTO lv_oh_region,
																																																																																																																																																																										lv_oh_brand,
																																																																																																																																																																										lv_oh_duty,
																																																																																																																																																																										lv_oh_freight,
																																																																																																																																																																										lv_oh_oh_duty,
																																																																																																																																																																										lv_oh_nonduty,
																																																																																																																																																																										lv_oh_freight_duty,
																																																																																																																																																																										lv_oh_inv_org_id,
																																																																																																																																																																										lv_oh_country,
																																																																																																																																																																										lv_oh_addl_duty
																																																																																																																																																																					FROM apps.fnd_flex_value_sets oh_set,
																																																																																																																																																																										apps.fnd_flex_values_vl oh_val
																																																																																																																																																																				WHERE 1 = 1
																																																																																																																																																																						AND oh_set.flex_value_set_id = oh_val.flex_value_set_id
																																																																																																																																																																						AND oh_set.flex_value_set_name = 'XXD_CST_OH_ELEMENTS_VS'
																																																																																																																																																																						AND NVL (TRUNC (oh_val.start_date_active), TRUNC (sysdate)) <= TRUNC (sysdate)
																																																																																																																																																																						AND NVL (TRUNC (oh_val.end_date_active), TRUNC (sysdate)) >= TRUNC (sysdate)
																																																																																																																																																																						AND oh_val.enabled_flag = 'Y'
																																																																																																																																																																						AND oh_val.attribute1 = l.region
																																																																																																																																																																						AND oh_val.attribute2 = j.brand
																																																																																																																																																																						AND oh_val.attribute8 IS NULL
																																																																																																																																																																						AND oh_val.attribute9 IS NULL
																																																																																																																																																																						AND oh_val.attribute11 IS NULL
																																																																																																																																																																						AND oh_val.attribute12 IS NULL;

																																																																																																																																																														EXCEPTION
																																																																																																																																																																			WHEN NO_DATA_FOUND
																																																																																																																																																																			THEN
																																																																																																																																																																							-- write_log_prc ('No OHE For Country, Org, Department, Style Is Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																																																																																																							lv_oh_region := NULL;
																																																																																																																																																																							lv_oh_brand := NULL;
																																																																																																																																																																							lv_oh_duty  := NULL;
																																																																																																																																																																							lv_oh_freight := NULL;
																																																																																																																																																																							lv_oh_oh_duty := NULL;
																																																																																																																																																																							lv_oh_nonduty := NULL;
																																																																																																																																																																							lv_oh_freight_duty := NULL;
																																																																																																																																																																							lv_oh_inv_org_id := NULL;
																																																																																																																																																																							lv_oh_country := NULL;
																																																																																																																																																																							lv_oh_addl_duty := NULL;																																																																																																																																																																			

																																																																																																																																																																			WHEN OTHERS
																																																																																																																																																																			THEN
																																																																																																																																																																							lv_oh_region := NULL;
																																																																																																																																																																							lv_oh_brand := NULL;
																																																																																																																																																																							lv_oh_duty  := NULL;
																																																																																																																																																																							lv_oh_freight := NULL;
																																																																																																																																																																							lv_oh_oh_duty := NULL;
																																																																																																																																																																							lv_oh_nonduty := NULL;
																																																																																																																																																																							lv_oh_freight_duty := NULL;
																																																																																																																																																																							lv_oh_inv_org_id := NULL;
																																																																																																																																																																							lv_oh_country := NULL;
																																																																																																																																																																							lv_oh_addl_duty := NULL;
																																																																																																																																																																							write_log_prc ('OTHERS: No OHE For Country, Org, Department, Style Is Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);

																																																																																																																																																														END;

																																																																																																																																																										WHEN OTHERS
																																																																																																																																																										THEN
																																																																																																																																																														lv_oh_region := NULL;
																																																																																																																																																														lv_oh_brand := NULL;
																																																																																																																																																														lv_oh_duty  := NULL;
																																																																																																																																																														lv_oh_freight := NULL;
																																																																																																																																																														lv_oh_oh_duty := NULL;
																																																																																																																																																														lv_oh_nonduty := NULL;
																																																																																																																																																														lv_oh_freight_duty := NULL;
																																																																																																																																																														lv_oh_inv_org_id := NULL;
																																																																																																																																																														lv_oh_country := NULL;
																																																																																																																																																														lv_oh_addl_duty := NULL;
																																																																																																																																																														write_log_prc ('OTHERS: No OHE For Country Not Null, Org, Department, Style Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);

																																																																																																																																																					END;																																																																																																																																																	

																																																																																																																																												     WHEN OTHERS
																																																																																																																																																	THEN
																																																																																																																																																					lv_oh_region := NULL;
																																																																																																																																																					lv_oh_brand := NULL;
																																																																																																																																																					lv_oh_duty  := NULL;
																																																																																																																																																					lv_oh_freight := NULL;
																																																																																																																																																					lv_oh_oh_duty := NULL;
																																																																																																																																																					lv_oh_nonduty := NULL;
																																																																																																																																																					lv_oh_freight_duty := NULL;
																																																																																																																																																					lv_oh_inv_org_id := NULL;
																																																																																																																																																					lv_oh_country := NULL;
																																																																																																																																																					lv_oh_addl_duty := NULL;
																																																																																																																																																					write_log_prc ('OTHERS: No OHE For Country, Department, Style Is Null Org Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);

																																																																																																																																												END;

																																																																																																																																								WHEN OTHERS
																																																																																																																																								THEN
																																																																																																																																												lv_oh_region := NULL;
																																																																																																																																												lv_oh_brand := NULL;
																																																																																																																																												lv_oh_duty  := NULL;
																																																																																																																																												lv_oh_freight := NULL;
																																																																																																																																												lv_oh_oh_duty := NULL;
																																																																																																																																												lv_oh_nonduty := NULL;
																																																																																																																																												lv_oh_freight_duty := NULL;
																																																																																																																																												lv_oh_inv_org_id := NULL;
																																																																																																																																												lv_oh_country := NULL;
																																																																																																																																												lv_oh_addl_duty := NULL;
																																																																																																																																												write_log_prc ('OTHERS: No OHE For Country, Org Not Null, Department, Style Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);

																																																																																																																																		 END;

																																																																																																																															WHEN OTHERS
																																																																																																																															THEN
																																																																																																																																			lv_oh_region := NULL;
																																																																																																																																			lv_oh_brand := NULL;
																																																																																																																																			lv_oh_duty  := NULL;
																																																																																																																																			lv_oh_freight := NULL;
																																																																																																																																			lv_oh_oh_duty := NULL;
																																																																																																																																			lv_oh_nonduty := NULL;
																																																																																																																																			lv_oh_freight_duty := NULL;
																																																																																																																																			lv_oh_inv_org_id := NULL;
																																																																																																																																			lv_oh_country := NULL;
																																																																																																																																			lv_oh_addl_duty := NULL;
																																																																																																																																			write_log_prc ('OTHERS: No OHE For Country, Style, Org Is Null Department Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);

																																																																																																																									 END;

																																																																																																																						WHEN OTHERS
																																																																																																																						THEN
																																																																																																																										lv_oh_region := NULL;
																																																																																																																										lv_oh_brand := NULL;
																																																																																																																										lv_oh_duty  := NULL;
																																																																																																																										lv_oh_freight := NULL;
																																																																																																																										lv_oh_oh_duty := NULL;
																																																																																																																										lv_oh_nonduty := NULL;
																																																																																																																										lv_oh_freight_duty := NULL;
																																																																																																																										lv_oh_inv_org_id := NULL;
																																																																																																																										lv_oh_country := NULL;
																																																																																																																										lv_oh_addl_duty := NULL;
																																																																																																																										write_log_prc ('OTHERS: No OHE For Country, Department Not Null, Org, Style Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);

																																																																																																																	END;

																																																																																																													WHEN OTHERS
																																																																																																													THEN
																																																																																																																	lv_oh_region := NULL;
																																																																																																																	lv_oh_brand := NULL;
																																																																																																																	lv_oh_duty  := NULL;
																																																																																																																	lv_oh_freight := NULL;
																																																																																																																	lv_oh_oh_duty := NULL;
																																																																																																																	lv_oh_nonduty := NULL;
																																																																																																																	lv_oh_freight_duty := NULL;
																																																																																																																	lv_oh_inv_org_id := NULL;
																																																																																																																	lv_oh_country := NULL;
																																																																																																																	lv_oh_addl_duty := NULL;
																																																																																																																	write_log_prc ('OTHERS: No OHE For Country, Style Null, Org, Department Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																																																							 END;

																																																																																																				WHEN OTHERS
																																																																																																				THEN
																																																																																																								lv_oh_region := NULL;
																																																																																																								lv_oh_brand := NULL;
																																																																																																								lv_oh_duty  := NULL;
																																																																																																								lv_oh_freight := NULL;
																																																																																																								lv_oh_oh_duty := NULL;
																																																																																																								lv_oh_nonduty := NULL;
																																																																																																								lv_oh_freight_duty := NULL;
																																																																																																								lv_oh_inv_org_id := NULL;
																																																																																																								lv_oh_country := NULL;
																																																																																																								lv_oh_addl_duty := NULL;
																																																																																																								write_log_prc ('OTHERS: No OHE For Country, Org, Department, Not Null, Style Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);

																																																																																															END;		

																																																																																											WHEN OTHERS
																																																																																											THEN
																																																																																															lv_oh_region := NULL;
																																																																																															lv_oh_brand := NULL;
																																																																																															lv_oh_duty  := NULL;
																																																																																															lv_oh_freight := NULL;
																																																																																															lv_oh_oh_duty := NULL;
																																																																																															lv_oh_nonduty := NULL;
																																																																																															lv_oh_freight_duty := NULL;
																																																																																															lv_oh_inv_org_id := NULL;
																																																																																															lv_oh_country := NULL;
																																																																																															lv_oh_addl_duty := NULL;
																																																																																															write_log_prc ('OTHERS: No OHE For Country, Org, Department Null, Style Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);

																																																																																					 END;

																																																																																		WHEN OTHERS
																																																																																		THEN
																																																																																						lv_oh_region := NULL;
																																																																																						lv_oh_brand := NULL;
																																																																																						lv_oh_duty  := NULL;
																																																																																						lv_oh_freight := NULL;
																																																																																						lv_oh_oh_duty := NULL;
																																																																																						lv_oh_nonduty := NULL;
																																																																																						lv_oh_freight_duty := NULL;
																																																																																						lv_oh_inv_org_id := NULL;
																																																																																						lv_oh_country := NULL;
																																																																																						lv_oh_addl_duty := NULL;
																																																																																						write_log_prc ('OTHERS: No OHE For Country, Style Not Null, Org, Department Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);

																																																																													END;

																																																																									WHEN OTHERS
																																																																									THEN
																																																																													lv_oh_region := NULL;
																																																																													lv_oh_brand := NULL;
																																																																													lv_oh_duty  := NULL;
																																																																													lv_oh_freight := NULL;
																																																																													lv_oh_oh_duty := NULL;
																																																																													lv_oh_nonduty := NULL;
																																																																													lv_oh_freight_duty := NULL;
																																																																													lv_oh_inv_org_id := NULL;
																																																																													lv_oh_country := NULL;
																																																																													lv_oh_addl_duty := NULL;
																																																																									    write_log_prc ('OTHERS: No OHE For Country, Department Null, Org, Style Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																																			 END;

																																																																WHEN OTHERS
																																																																THEN
																																																																				lv_oh_region := NULL;
																																																																				lv_oh_brand := NULL;
																																																																				lv_oh_duty  := NULL;
																																																																				lv_oh_freight := NULL;
																																																																				lv_oh_oh_duty := NULL;
																																																																				lv_oh_nonduty := NULL;
																																																																				lv_oh_freight_duty := NULL;
																																																																				lv_oh_inv_org_id := NULL;
																																																																				lv_oh_country := NULL;
																																																																				lv_oh_addl_duty := NULL;
																																																																				write_log_prc ('OTHERS: No OHE For Country, Org, Style Not Null, Department Is Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																										 END;

																																																							WHEN OTHERS
																																																							THEN
																																																											lv_oh_region := NULL;
																																																											lv_oh_brand := NULL;
																																																											lv_oh_duty  := NULL;
																																																											lv_oh_freight := NULL;
																																																											lv_oh_oh_duty := NULL;
																																																											lv_oh_nonduty := NULL;
																																																											lv_oh_freight_duty := NULL;
																																																											lv_oh_inv_org_id := NULL;
																																																											lv_oh_country := NULL;
																																																											lv_oh_addl_duty := NULL;
																																																											write_log_prc ('OTHERS: No OHE For Country, Org Null, Department, Style Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																																		END;

																																														WHEN OTHERS
																																														THEN
																																																		lv_oh_region := NULL;
																																																		lv_oh_brand := NULL;
																																																		lv_oh_duty  := NULL;
																																																		lv_oh_freight := NULL;
																																																		lv_oh_oh_duty := NULL;
																																																		lv_oh_nonduty := NULL;
																																																		lv_oh_freight_duty := NULL;
																																																		lv_oh_inv_org_id := NULL;
																																																		lv_oh_country := NULL;
																																																		lv_oh_addl_duty := NULL;
																																																		write_log_prc ('OTHERS: No OHE For Country, Department, Style Not Null, Org Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);

																																									END;

																																					WHEN OTHERS
																																					THEN
																																									lv_oh_region := NULL;
																																									lv_oh_brand := NULL;
																																									lv_oh_duty  := NULL;
																																									lv_oh_freight := NULL;
																																									lv_oh_oh_duty := NULL;
																																									lv_oh_nonduty := NULL;
																																									lv_oh_freight_duty := NULL;
																																									lv_oh_inv_org_id := NULL;
																																									lv_oh_country := NULL;
																																									lv_oh_addl_duty := NULL;
																																					    write_log_prc ('OTHERS: No OHE For Country Null, Org, Department, Style Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);
																																END;

																												WHEN OTHERS
																												THEN
																																lv_oh_region := NULL;
																																lv_oh_brand := NULL;
																																lv_oh_duty  := NULL;
																																lv_oh_freight := NULL;
																																lv_oh_oh_duty := NULL;
																																lv_oh_nonduty := NULL;
																																lv_oh_freight_duty := NULL;
																																lv_oh_inv_org_id := NULL;
																																lv_oh_country := NULL;
																																lv_oh_addl_duty := NULL;
                                write_log_prc ('OTHERS: No OHE For Country Org, Department, Style Not Null'||l.country_code||'-'||l.inventory_org_id||'-'||i.department||'-'||i.style_number);																															
																							END;

																							-- IF i.rec_status = 'V'
																							-- THEN

																											FOR k IN cur_comm_items (i.style_number, lv_oh_brand, l.inventory_org_id, i.department)
																											LOOP

																															-- write_log_prc ('inv_item_id:'||k.inv_item_id||'brand:'||lv_oh_brand||'style number:'||i.style_number||'department:'||i.department);

																															-- IF pv_duty_override = 'Y' AND i.preferential_rate = 'Y' AND i.duty_rate IS NULL
																															-- THEN
																																			-- lv_duty := NULL;
																																			-- lv_duty := lv_oh_duty;
																															-- ELSIF pv_duty_override = 'Y' AND i.preferential_rate = 'N' AND i.default_duty_rate IS NULL
																															-- THEN
																																			-- lv_duty := NULL;
																																			-- lv_duty := lv_oh_duty;
																															-- ELSIF pv_duty_override = 'N' AND i.preferential_rate = 'Y' AND i.duty_rate IS NOT NULL
																															-- THEN
																																			-- lv_duty := NULL;
																																			-- lv_duty := i.duty_rate;
																															-- ELSIF pv_duty_override = 'N' AND i.preferential_rate = 'N' AND i.default_duty_rate IS NOT NULL
																															-- THEN
																																			-- lv_duty := NULL;
																																			-- lv_duty := i.default_duty_rate;
																															-- END IF;

-- Begin CCR0009885 
/*																															IF i.preferential_rate = 'Y' AND i.duty_rate IS NOT NULL
																															THEN
																																			lv_duty := NULL;
																																			lv_duty := i.duty_rate;
																															ELSIF i.preferential_rate = 'N' AND i.default_duty_rate IS NOT NULL
																															THEN
																																   lv_duty := NULL;
																																			lv_duty := i.default_duty_rate;
																															END IF;

																															IF i.duty_rate IS NULL
																															THEN */

																															IF lv_oh_duty IS NOT NULL
																															THEN
																															    lv_duty := NULL;
																															    lv_duty :=  lv_oh_duty;

                                   IF lv_oh_duty = 0 
                                   THEN 
                                      i.duty_rate         := lv_oh_duty; --Added for CCR0010051
                                      i.default_duty_rate := lv_oh_duty; --Added for CCR0010051
                                   END IF;


																															ELSIF i.preferential_rate = 'Y' AND i.duty_rate IS NOT NULL AND lv_oh_duty IS NULL
																															THEN
																																			lv_duty := NULL;
																																			lv_duty := i.duty_rate;
																															ELSIF i.preferential_rate = 'N' AND i.default_duty_rate IS NOT NULL AND lv_oh_duty IS NULL
																															THEN
																																   lv_duty := NULL;
																																			lv_duty := i.default_duty_rate;
																															END IF;

																															IF i.duty_rate IS NULL AND i.default_duty_rate IS NULL AND lv_oh_duty IS NULL
																															THEN
-- End CCR0009885		
																															    lv_duty := NULL;
																															    BEGIN
																																			     SELECT DISTINCT usage_rate_or_amount
																																								  INTO lv_duty
																																								  FROM cst_item_cost_details_v cicd
																																									WHERE inventory_item_id = k.inv_item_id
																																									  AND organization_id = l.inventory_org_id
																																									  AND cost_type_id = 1000
                                           AND resource_code = 'DUTY';																																			

																																			EXCEPTION
																																			     WHEN OTHERS
																																								THEN
																																								    lv_duty := NULL;																																								
																																			END;

																														 END IF;

																															BEGIN
																																				-- write_log_prc ('Create Element Record with Derived columns and Insert into Stg Table');
																																				INSERT INTO xxdo.xxd_cst_duty_ele_upld_stg_t (
																																																																																		region
																																																																																	,style_number
																																																																																	,style_color
																																																																																	,item_size
																																																																																	,organization_code
																																																																																	,item_number
																																																																																	,country_of_origin
																																																																																	,destination_country
																																																																																	,department
																																																																																	,duty_rate
																																																																																	,additional_duty
																																																																																	,applicable_uom
																																																																																	,preferential_rate
																																																																																	,preferential_duty_flag
																																																																																	,pr_start_date
																																																																																	,pr_end_date
																																																																																	,default_duty_rate
																																																																																	,default_additional_duty
																																																																																	,default_applicable_uom
																																																																																	,effective_start_date
																																																																																	,effective_end_date
																																																																																	,duty
																																																																																	,primary_duty_flag
																																																																																	,duty_start_date
																																																																																	,duty_end_date
																																																																																	,freight
																																																																																	,freight_duty
																																																																																	,oh_duty
																																																																																	,oh_nonduty
																																																																																	,factory_cost
																																																																																	,addl_duty
																																																																																	,tarrif_code
																																																																																	,country
																																																																																	,default_category
																																																																																	,operating_unit
																																																																																	,inventory_org_id
																																																																																	,inventory_item_id
																																																																																	,coo_precedence
																																																																																	,coo_preference_flag
																																																																																	,group_id
																																																																																	,duty_override
																																																																																	,rec_status
																																																																																	,active_flag
																																																																																	,status_category
																																																																																	,error_msg
																																																																																	,created_by
																																																																																	,creation_date
																																																																																	,last_update_date
																																																																																	,last_updated_by
																																																																																	,request_id
																																																																																	,filename
																																																																																	,additional_field1
																																																																																	)
																																																																										VALUES 
																																																																																	(TRIM (l.region)
																																																																																	,TRIM (k.style_number)
																																																																																	,TRIM (k.style_color)
																																																																																	,TRIM (k.item_size)
																																																																																	,TRIM (lv_organization_code)
																																																																																	,TRIM (k.item_number)
																																																																																	,TRIM (i.country_of_origin)
																																																																																	,TRIM (i.destination_country)
																																																																																	,TRIM (UPPER (i.department))
																																																																																	,TRIM (i.duty_rate)
																																																																																	,TRIM (i.additional_duty)
																																																																																	,TRIM (i.applicable_uom)
																																																																																	,TRIM (i.preferential_rate)
																																																																																	,TRIM (i.preferential_duty_flag)
																																																																																	,TRIM (i.pr_start_date)
																																																																																	,TRIM (i.pr_end_date)
																																																																																	,TRIM (i.default_duty_rate)
																																																																																	,TRIM (i.default_additional_duty)
																																																																																	,TRIM (i.default_applicable_uom)
																																																																																	,TRIM (i.effective_start_date)
																																																																																	,TRIM (i.effective_end_date)																																																																											
																																																																																	,TRIM (lv_duty)
																																																																															--,'Y'             --Commented for CCR0010051
																																																																																	,'N'             --Added for CCR0010051
																																																																																	,NULL
																																																																																	,NULL
																																																																																	,TRIM (lv_oh_freight)
																																																																																	,TRIM (lv_oh_freight_duty)
																																																																																	,TRIM (lv_oh_oh_duty)
																																																																																	,TRIM (lv_oh_nonduty)
																																																																																	,NULL
																																																																																	,TRIM (lv_oh_addl_duty)
																																																																																	,NULL -- i.hts_code
																																																																																	,TRIM (l.country)
																																																																																	,NULL
																																																																																	,TRIM (lv_operating_unit)
																																																																																	,TRIM (l.inventory_org_id)
																																																																																	,TRIM (k.inv_item_id)
																																																																																	,TRIM (l.coo_precedence)
																																																																																	--,NVL (TRIM (l.coo_preference_flag), 'N')
																																																																																	,'N'
																																																																																	,TRIM (l_group_id)
																																																																																	--,TRIM (pv_duty_override)
																																																																																	,NULL
																																																																																	,'N'
																																																																																	,TRIM (i.active_flag)
																																																																																	,'N'
																																																																																	,TRIM (i.error_msg)
																																																																																	,TRIM (gn_user_id)
																																																																																	,SYSDATE
																																																																																	,SYSDATE
																																																																																	,TRIM (gn_user_id)
																																																																																	,TRIM (gn_request_id)
																																																																																	,TRIM (i.filename)
																																																																																	,TRIM (lv_oh_brand));
																															EXCEPTION
																																				WHEN OTHERS 
																																				THEN
																																								write_log_prc (SQLERRM||' Insertion Failed for Staging table: xxdo.xxd_cst_duty_ele_upld_stg_t');
																															END;

																																			--i.error_msg := '';

																											EXIT WHEN cur_comm_items%NOTFOUND;
																											END LOOP;
																											COMMIT;

																							-- END IF;

																							EXIT WHEN cur_oh_ele_data%NOTFOUND;
																							END LOOP;
																							COMMIT;

																			-- END IF;
																			EXIT WHEN cur_derive_regn_cnty_org%NOTFOUND;
																			END LOOP;
																			COMMIT;

															EXIT WHEN cur_tr_data%NOTFOUND;
															END LOOP;											
															COMMIT;

 										BEGIN

																UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t s
																			SET rec_status = 'P'
																	WHERE rec_status = 'V'
																			AND active_flag = 'Y'
																			AND error_msg IS NULL
																			AND EXISTS (SELECT 1 
																																	FROM xxdo.xxd_cst_duty_ele_upld_stg_t u
																																WHERE 1 = 1
																																		AND s.style_number = u.style_number
																																		AND s.country_of_origin = u.country_of_origin
																																		AND s.destination_country = u.destination_country
																																		AND u.active_flag ='Y'
																																		AND u.rec_status = 'N'
																																		AND u.error_msg IS NULL)
																		 AND request_id = gn_request_id;

                write_log_prc (SQL%ROWCOUNT||' Records Updated in Inbound Stg table as Processed');
																COMMIT;

											EXCEPTION
																WHEN OTHERS 
																THEN
																				write_log_prc ('Procesed: Updation Failed for Staging table: xxdo.xxd_cst_duty_ele_inb_stg_tr_t'||SQLERRM);
											END;

											BEGIN

																UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t s
																			SET rec_status = 'E'
																			   ,error_msg = error_msg||'Unable To Derive OH Values From Value Set'
																	WHERE rec_status = 'V'
																			AND active_flag = 'Y'
																			AND NOT EXISTS (SELECT 1 
																																					FROM xxdo.xxd_cst_duty_ele_upld_stg_t u
																																				WHERE 1 = 1
																																						AND s.style_number = u.style_number
																																						AND s.country_of_origin = u.country_of_origin
																																						AND s.destination_country = u.destination_country
																																						AND u.rec_status = 'N'
																																						AND u.error_msg IS NULL)
                   AND request_id = gn_request_id;

                write_log_prc (SQL%ROWCOUNT||' Records Updated in Inbound Stg table as Errored');
																COMMIT;

											EXCEPTION
																WHEN OTHERS 
																THEN
																				write_log_prc ('Error: Updation Failed for Staging table: xxdo.xxd_cst_duty_ele_inb_stg_tr_t'||SQLERRM);
											END;

											BEGIN

																UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t s
																			SET rec_status = 'E'
																			   ,error_msg = error_msg||'Duty Element cannot be NULL'
																	WHERE rec_status = 'V'
																			AND active_flag = 'Y'
																			AND EXISTS (SELECT 1 
																																	FROM xxdo.xxd_cst_duty_ele_upld_stg_t u
																																WHERE 1 = 1
																																		AND s.style_number = u.style_number
																																		AND s.country_of_origin = u.country_of_origin
																																		AND s.destination_country = u.destination_country
																																		AND u.rec_status = 'N'
																																		AND u.error_msg IS NULL
																																		AND u.active_flag = 'Y'
																																		AND u.duty IS NULL)
                   AND request_id = gn_request_id;

                write_log_prc (SQL%ROWCOUNT||' Records Updated in Inbound Stg table as Errored since Duty is NULL');
																COMMIT;

											EXCEPTION
																WHEN OTHERS 
																THEN
																				write_log_prc ('Duty: Updation Failed for Staging table: xxdo.xxd_cst_duty_ele_inb_stg_tr_t'||SQLERRM);
											END;

											BEGIN
                -- Mark Not Applicable ('X') for Duty Element cannot be NULL in SKU stg table
																UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t
																			SET rec_status = 'X'
																			   ,error_msg = 'Duty Element cannot be NULL'
																	WHERE rec_status = 'N'
																	  AND error_msg IS NULL
																			AND active_flag = 'Y'
																			AND duty IS NULL
                   AND request_id = gn_request_id;

                write_log_prc (SQL%ROWCOUNT||' Records Updated in SKU Stg table as Errored since Duty is NULL');
																COMMIT;

											EXCEPTION
																WHEN OTHERS 
																THEN
																				write_log_prc ('Duty: Updation Failed for Staging table: xxdo.xxd_cst_duty_ele_upld_stg_t'||SQLERRM);
											END;

											BEGIN

																UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t t1
																			SET active_flag = 'N'
																	WHERE 1 = 1
																			AND active_flag = 'Y' 
																			AND (style_number, 
																								country_of_origin, 
																								destination_country)  IN (SELECT style_number, 
																																																									country_of_origin, 
																																																									destination_country
																																																				FROM xxdo.xxd_cst_duty_ele_upld_stg_t t2
																																																			WHERE 1 = 1
																																																					AND request_id = gn_request_id
																																																					AND active_flag = 'Y'
																																																					AND t1.style_number = t2.style_number
																																																					AND t1.country_of_origin = t2.country_of_origin
																																																					AND t1.destination_country = t2.destination_country)
																			AND NVL (request_id, gn_request_id) <> gn_request_id;

																write_log_prc ( SQL%ROWCOUNT || ' Element records updated with Active Flag as N ');
																COMMIT;

											EXCEPTION
																WHEN OTHERS 
																THEN
																				write_log_prc (' Updation Failed for active flag in Staging table: xxdo.xxd_cst_duty_ele_upld_stg_t'||SQLERRM);
											END;											
		/*	--BEGIN: Commented for CCR0010355
											BEGIN		

																UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t u
																			SET coo_preference_flag = 'Y'
																	WHERE EXISTS (SELECT s.style_number
																																					,s.region
																																					,s.country
																																					,s.inventory_org_id
																																					,s.coo_precedence FROM (SELECT style_number style_number
																																																																			,region region
																																																																			,country country
																																																																			,inventory_org_id inventory_org_id
																																																																			,MIN (coo_precedence) coo_precedence
																																																															FROM xxdo.xxd_cst_duty_ele_upld_stg_t
																																																														WHERE coo_precedence <> 1000
																																																														  AND request_id = gn_request_id
																																																																AND active_flag = 'Y'
																																																											GROUP BY style_number
																																																																			,region
																																																																			,country
																																																																			,inventory_org_id) s
																																WHERE s.style_number = u.style_number
																																		AND s.region = u.region
																																		AND s.country = u.country
																																		AND s.inventory_org_id = u.inventory_org_id
																																		AND s.coo_precedence = u.coo_precedence
																																		AND coo_precedence <> 1000
																																		AND u.request_id = gn_request_id
																																		AND u.active_flag = 'Y')
																			AND request_id = gn_request_id
--																			AND filename = pv_file_name
																			AND coo_precedence <> 1000
																			AND active_flag = 'Y';

																write_log_prc ( SQL%ROWCOUNT || ' Records updated COO preference flag as Y');
																COMMIT;

											EXCEPTION
																WHEN OTHERS 
																THEN
																				write_log_prc (' COO preference flag Updation Failed for precedence <> 1000'||SQLERRM);
											END;
		*/	--END: Commented for CCR0010355
										--	BEGIN
-- Begin CCR0009885
																/*UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t
																			SET coo_preference_flag = 'Y'
																	WHERE (style_number, 
																								country_of_origin) IN (SELECT style_number,
																																																						country_of_origin FROM (SELECT style_number,
																																																																																					country_of_origin, 
																																																																																					ROW_NUMBER() OVER (PARTITION BY inventory_org_id, style_number  ORDER BY style_number, inventory_org_id) rn 
																																																																																FROM (SELECT style_number, 
																																																																																													country_of_origin 
																																																																																								FROM xxdo.xxd_cst_duty_ele_upld_stg_t t1
																																																																																							WHERE coo_precedence = 1000
																																																																																									AND style_number NOT IN (SELECT style_number
																																																																																																																				FROM xxdo.xxd_cst_duty_ele_upld_stg_t t2
																																																																																																																			WHERE coo_precedence in (1,2,3,4,5))
																																																																												GROUP BY style_number, country_of_origin)
																																																																													)
																																																													WHERE rn = 1
																																														)
																			AND coo_preference_flag = 'N'
																			AND active_flag = 'Y';*/
		/*	--BEGIN: Commented for CCR0010355
																UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t
																			SET coo_preference_flag = 'Y'
																	WHERE (style_number, 
																								country_of_origin,
																								inventory_org_id) IN (SELECT style_number,
																																																					country_of_origin,
																																																					inventory_org_id FROM (SELECT style_number,
																																																																																			country_of_origin, 
																																																																																			inventory_org_id,
																																																																																			ROW_NUMBER() OVER (PARTITION BY style_number, inventory_org_id ORDER BY style_number, inventory_org_id) rn 
																																																																													 FROM (SELECT style_number, 
																																																																																											country_of_origin,
																																																																																											inventory_org_id																																																																																											
																																																																																					 FROM xxdo.xxd_cst_duty_ele_upld_stg_t t1
																																																																																					WHERE coo_precedence = 1000
																																																																																					  AND request_id = gn_request_id
																																																																																							AND active_flag = 'Y'
																																																																																							-- AND (style_number, inventory_org_id) NOT IN (SELECT style_number, inventory_org_id
																																																																																																																																						-- FROM xxdo.xxd_cst_duty_ele_upld_stg_t t2
																																																																																																																																					-- WHERE coo_precedence IN (1,2,3,4,5)
																																																																																																																																					  -- AND request_id = gn_request_id
																																																																																																																																							-- AND active_flag = 'Y')
																																																																												GROUP BY style_number, 
																																																																												         country_of_origin,
																																																																																					inventory_org_id)
																																																																													)
																																																													WHERE rn = 1
																																														)
																			AND coo_preference_flag = 'N'
																			AND active_flag = 'Y'
																			AND request_id = gn_request_id;

-- End CCR0009885

																write_log_prc ( SQL%ROWCOUNT || ' records updated with coo_preference_flag as Y ');
																COMMIT;

											EXCEPTION
																WHEN OTHERS 
																THEN
																				write_log_prc (' COO preference flag Updation Failed for precedence = 1000'||SQLERRM);
											END;
		*/	--END: Commented for CCR0010355
         --BEGIN: Added for CCR0010355
          BEGIN

				-- Update COO Preference Flag as N for all the records
              UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t
                 SET coo_preference_flag = 'N'
				   , primary_duty_flag = 'N'
			   WHERE request_id = gn_request_id;
              write_log_prc('COO Preference and Primary Duty Flag Marked N:-' || SQL%ROWCOUNT);
              COMMIT;

              --Update COO Preference Flag as Y for coo_precedence <> 1000
              UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t a
                 SET coo_preference_flag = 'Y'
                   , primary_duty_flag = 'Y'
				   , last_update_date  = SYSDATE 
               WHERE 1=1 
                 AND active_flag = 'Y'
                 AND a.coo_precedence != 1000 
				 AND a.request_id = gn_request_id
                 AND a.coo_precedence = (SELECT MIN(coo_precedence)
                                           FROM xxd_cst_duty_ele_upld_stg_t b
                                          WHERE 1=1
                                            AND b.active_flag = 'Y'
                                            AND b.coo_precedence != 1000 
                                            AND b.inventory_org_id  = a.inventory_org_id
                                            AND b.inventory_item_id = a.inventory_item_id)
				AND a.coo_precedence <= (SELECT MIN(coo_precedence)
                                           FROM xxdo_invval_duty_cost c
                                          WHERE 1=1
                                            AND c.country_of_origin = a.country_of_origin
                                            AND c.inventory_org  = a.inventory_org_id
                                            AND c.inventory_item_id = a.inventory_item_id
											AND NVL (c.duty_end_date, TRUNC (SYSDATE+1)) >= TRUNC (SYSDATE);										;                                                                 
              write_log_prc(SQL%ROWCOUNT || ' Records updated COO Preference and Primary Duty Flag as Y for coo_precedence <> 1000');
              COMMIT;
              -- 
              --Update COO Preference Flag as Y for coo_precedence = 1000
              UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t a
                 SET coo_preference_flag = 'Y'
                   , last_update_date  = SYSDATE 
               WHERE 1=1 
                 AND active_flag = 'Y'
                 AND a.coo_precedence = 1000 
				 AND a.request_id = gn_request_id
                 AND ROWID = (SELECT MAX(ROWID)
                                FROM xxdo.xxd_cst_duty_ele_upld_stg_t b
                               WHERE 1=1
                                 AND b.active_flag       = 'Y'
                                 AND b.coo_precedence    = 1000
								 AND b.region  = a.region
								 AND b.country  = a.country
								 AND b.style_number  = a.style_number
								 AND b.request_id = gn_request_id)
                 AND 0 = (SELECT COUNT(DISTINCT 1)
						    FROM xxdo.xxd_cst_duty_ele_upld_stg_t c
						   WHERE 1=1 
						     AND c.region = a.region
						     AND c.country = a.country
						     AND c.inventory_org_id = a.inventory_org_id
						     AND c.style_number = a.style_number
						     AND c.coo_precedence <> 1000
						     AND c.active_flag = 'Y'
						  );
              write_log_prc(SQL%ROWCOUNT || ' Records updated COO preference flag as Y for coo_precedence = 1000');
              COMMIT;

			EXCEPTION
			WHEN OTHERS THEN
					write_log_prc ('coo_preference_flag Updation Failed: '||SQLERRM);
			END;
           --END: Added for CCR0010355		
         --BEGIN: Added for CCR0010051
          BEGIN
	/* -- BEGIN: Commented for CCR0010355
															-- Update Primary duty falg as N for all the records
              UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t
                 SET primary_duty_flag = 'N';
              write_log_prc('Primary Duty Flag Marked N:-' || SQL%ROWCOUNT);
              COMMIT;
              --


              --Update Primary Duty Flag as Y for coo_precedence <> 1000
              UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t a
                 SET primary_duty_flag = 'Y'
                   , last_update_date  = SYSDATE 
               WHERE 1=1 
                 AND active_flag = 'Y'
                 AND a.coo_precedence != 1000 
                 AND coo_precedence = (SELECT MIN(coo_precedence)
                                         FROM xxd_cst_duty_ele_upld_stg_t b
                                        WHERE 1=1
                                          AND b.active_flag = 'Y'
                                          AND b.coo_precedence != 1000 
                                          AND b.inventory_org_id  = a.inventory_org_id
                                          AND b.inventory_item_id = a.inventory_item_id);                                                                 
              write_log_prc('Update Primary Duty Flag as Y for coo_precedence <> 1000 - '|| SQL%ROWCOUNT);
              COMMIT;
              -- 
	*/	-- END: Commented for CCR0010355
              --Update Primary Duty Flag as Y for coo_precedence = 1000
              UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t a
                 SET primary_duty_flag = 'Y'
                   , last_update_date  = SYSDATE 
               WHERE 1=1 
                 AND active_flag = 'Y'
                 AND a.coo_precedence = 1000
				 AND a.request_id = gn_request_id	-- Added for CCR0010355 
                 AND ROWID = (SELECT MAX(ROWID)
                                FROM xxdo.xxd_cst_duty_ele_upld_stg_t b
                               WHERE 1=1
                                 AND b.active_flag       = 'Y'
                                 AND b.coo_precedence    = 1000
								 AND b.request_id = gn_request_id		-- Added for CCR0010355
                                 AND b.inventory_org_id  = a.inventory_org_id
                                 AND b.inventory_item_id = a.inventory_item_id)
                 AND NOT EXISTS(SELECT DISTINCT 1						-- Added DISTINCT for CCR0010355
                                  FROM xxdo.xxd_cst_duty_ele_upld_stg_t b
                                 WHERE 1=1
                                   AND b.active_flag = 'Y'
                                   AND b.coo_precedence <> 1000 
                                   AND b.inventory_org_id  = a.inventory_org_id
                                   AND b.inventory_item_id = a.inventory_item_id); 
              write_log_prc('Update Primary Duty Flag as Y for coo_precedence = 1000 - '|| SQL%ROWCOUNT);
              COMMIT;
              -- 

											EXCEPTION
																WHEN OTHERS 
																THEN
																				write_log_prc ('Primary Duty Flag Updation Failed: '||SQLERRM);
											END;
           --END: Added for CCR0010051



       ELSIF ln_cnt = 0
       THEN
							    write_log_prc ('No Valid records are present in the xxdo.xxd_cst_duty_ele_inb_stg_tr_t table and SQLERRM' || SQLERRM);
											apps.fnd_file.put_line(apps.fnd_file.log,'');
											apps.fnd_file.put_line(apps.fnd_file.log,'No Valid records are present in the xxdo.xxd_cst_duty_ele_inb_stg_tr_t table');
											apps.fnd_file.put_line(apps.fnd_file.log,'');
							END IF;

							-- insert_cat_to_stg_prc (xv_errbuf
							                      -- ,xv_retcode);

							write_log_prc('Procedure insert_into_custom_table_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

		EXCEPTION
       WHEN	OTHERS
							THEN
							    write_log_prc ('Error in Procedure insert_into_custom_table_prc:' || SQLERRM);
  END insert_into_custom_table_prc;

	/***************************************************************************
	-- PROCEDURE create_final_zip_prc
	-- PURPOSE: This Procedure Converts the file to zip file
	***************************************************************************/

	FUNCTION file_to_blob_fnc (pv_directory_name   IN VARCHAR2,
																												pv_file_name        IN VARCHAR2)
					RETURN BLOB
	IS
					dest_loc   BLOB := EMPTY_BLOB ();
					src_loc    BFILE := BFILENAME (pv_directory_name, pv_file_name);
	BEGIN

					fnd_file.put_line(fnd_file.log,' Start of Convering the file to BLOB');

					DBMS_LOB.OPEN (src_loc, DBMS_LOB.LOB_READONLY);

					DBMS_LOB.CREATETEMPORARY (lob_loc   => dest_loc,
																															cache     => TRUE,
																															dur       => DBMS_LOB.session);

					DBMS_LOB.OPEN (dest_loc, DBMS_LOB.LOB_READWRITE);

					DBMS_LOB.LOADFROMFILE (dest_lob   => dest_loc,
																												src_lob    => src_loc,
																												amount     => DBMS_LOB.getLength (src_loc));

					DBMS_LOB.CLOSE (dest_loc);

					DBMS_LOB.CLOSE (src_loc);

					RETURN dest_loc;
	EXCEPTION
					WHEN OTHERS
					THEN

									fnd_file.put_line(fnd_file.log,' Exception in Converting the file to BLOB - '||SQLERRM);    

									RETURN NULL;

	END file_to_blob_fnc;

	PROCEDURE save_zip_prc (pb_zipped_blob     BLOB,
																									pv_dir             VARCHAR2,
																									pv_zip_file_name   VARCHAR2)
	IS
					t_fh    UTL_FILE.file_type;
					t_len   PLS_INTEGER := 32767;
	BEGIN
					fnd_file.put_line(fnd_file.log,' Start of save_zip_prc Procedure');

					dbms_output.put_line(' Start of save_zip_prc Procedure');

					t_fh := UTL_FILE.fopen (pv_dir, pv_zip_file_name, 'wb');

					dbms_output.put_line(' Start of save_zip_prc Procedure');

					FOR i IN 0 ..
														TRUNC ((DBMS_LOB.getlength (pb_zipped_blob) - 1) / t_len)
					LOOP
									UTL_FILE.put_raw (
													t_fh,
													DBMS_LOB.SUBSTR (pb_zipped_blob, t_len, i * t_len + 1));
					END LOOP;

					UTL_FILE.fclose (t_fh);
	EXCEPTION
	WHEN OTHERS
	THEN
					NULL;
					fnd_file.put_line(fnd_file.log,' Exception in save_zip_prc Procedure - '||SQLERRM);

					dbms_output.put_line(' Exception in save_zip_prc Procedure - '||SQLERRM);

	END save_zip_prc;


	PROCEDURE create_final_zip_prc (pv_directory_name   IN VARCHAR2,
																																	pv_file_name        IN VARCHAR2,
																																	pv_zip_file_name    IN VARCHAR2)
	IS
					lb_file   BLOB;
					lb_zip    BLOB;
	BEGIN

					fnd_file.put_line(fnd_file.log,' Start of file_to_blob_fnc ');

					lb_file := file_to_blob_fnc (pv_directory_name, pv_file_name);

					fnd_file.put_line(fnd_file.log,pv_directory_name||pv_file_name);

					fnd_file.put_line(fnd_file.log,' Start of add_file PROC ');

					APEX_200200.WWV_FLOW_ZIP.add_file (lb_zip,
																																								pv_file_name,
																																								lb_file);

					fnd_file.put_line(fnd_file.log,' Start of finish PROC ');

					APEX_200200.wwv_flow_zip.finish (lb_zip);

					fnd_file.put_line(fnd_file.log,' Start of Saving ZIP File PROC ');

					save_zip_prc (lb_zip, pv_directory_name, pv_zip_file_name);

	END create_final_zip_prc;

	/***************************************************************************
	-- PROCEDURE generate_exception_report_prc
	-- PURPOSE: This Procedure generate the output and write the file 
	-- into Exception directory
	**************************************************************************/ 
	PROCEDURE generate_exception_report_prc (--pv_file_name     IN    VARCHAR2,
	                                         pv_exc_file_name   OUT VARCHAR2)
	IS    

	  CURSOR exception_hdr
			IS
					SELECT				'Style Number'
												|| gv_delim_pipe
												|| 'Style Description'
												|| gv_delim_pipe
												|| 'Country Of Origin'
												|| gv_delim_pipe
												|| 'Hts Code'
												|| gv_delim_pipe
												|| 'Duty Rate'
												|| gv_delim_pipe
												|| 'Additional Duty'
												|| gv_delim_pipe
												|| 'Applicable UOM'
												|| gv_delim_pipe
												|| 'Destination Country'
												|| gv_delim_pipe
												|| 'Preferential Rate'
												|| gv_delim_pipe
												|| 'PR Start Date'
												|| gv_delim_pipe
												|| 'PR End Date'
												|| gv_delim_pipe
												|| 'Default Duty Rate'
												|| gv_delim_pipe
												|| 'Default Additional Duty'
												|| gv_delim_pipe
												|| 'Default Applicable UOM'
												|| gv_delim_pipe
												|| 'Effective Start Date'
												|| gv_delim_pipe
												|| 'Effective End Date'
												|| gv_delim_pipe
												|| 'Department'
												|| gv_delim_pipe
												|| 'Data Source'
												|| gv_delim_pipe
												|| 'Other Vendor Sites'
												|| gv_delim_pipe
												|| 'Intro Season'
												|| gv_delim_pipe
												|| 'Size Run'
												|| gv_delim_pipe
												|| 'Pd Assigned'
												|| gv_delim_pipe
												|| 'Collection'
												|| gv_delim_pipe
												|| 'Tarrif Description'
												|| gv_delim_pipe
												|| 'Item Category'
												|| gv_delim_pipe
												|| 'Additional Field1'
												|| gv_delim_pipe
												|| 'Additional Field2'
												|| gv_delim_pipe
												|| 'Additional Field3'
												|| gv_delim_pipe
												|| 'Additional Field4'
												|| gv_delim_pipe
												|| 'Additional Field5'
												|| gv_delim_pipe
												|| 'Additional Field6'
												|| gv_delim_pipe
												|| 'Additional Field7'
												|| gv_delim_pipe
												|| 'Additional Field8'
												|| gv_delim_pipe
												|| 'Additional Field9'
												|| gv_delim_pipe
												|| 'Additional Field10'
												|| gv_delim_pipe
												|| 'Additional Field11'
												|| gv_delim_pipe
												|| 'Additional Field12'
												|| gv_delim_pipe
												|| 'Additional Field13'
												|| gv_delim_pipe
												|| 'Additional Field14'
												|| gv_delim_pipe
												|| 'Additional Field15'
												|| gv_delim_pipe
												|| 'Additional Field16'
												|| gv_delim_pipe
												|| 'Additional Field17'
												|| gv_delim_pipe
												|| 'Additional Field18'
												|| gv_delim_pipe
												|| 'Additional Field19'
												|| gv_delim_pipe
												|| 'Additional Field20'				
												|| gv_delim_pipe
												|| 'Current Flag'
												|| gv_delim_pipe
												|| 'Preferential Duty Flag'
												|| gv_delim_pipe
												|| 'Record Status'
												|| gv_delim_pipe
												|| 'Error Msg'
												|| gv_delim_pipe
												|| 'File Name' line_hdr
							FROM DUAL;

			CURSOR exception_rec
			IS
			  SELECT 	TRIM (style_number)
											|| gv_delim_pipe
											|| TRIM (style_description)
											|| gv_delim_pipe
											|| TRIM (country_of_origin)
											|| gv_delim_pipe
											|| TRIM (hts_code)
											|| gv_delim_pipe
											|| TRIM (duty_rate)
											|| gv_delim_pipe
											|| TRIM (additional_duty)
											|| gv_delim_pipe
											|| TRIM (applicable_uom)
											|| gv_delim_pipe
											|| TRIM (destination_country)
											|| gv_delim_pipe
											|| TRIM (preferential_rate)
											|| gv_delim_pipe
											|| TRIM (pr_start_date)
											|| gv_delim_pipe
											|| TRIM (pr_end_date)
											|| gv_delim_pipe
											|| TRIM (default_duty_rate)
											|| gv_delim_pipe
											|| TRIM (default_additional_duty)
											|| gv_delim_pipe
											|| TRIM (default_applicable_uom)
											|| gv_delim_pipe
											|| TRIM (effective_start_date)
											|| gv_delim_pipe
											|| TRIM (effective_end_date)
											|| gv_delim_pipe
											|| TRIM (department)
											|| gv_delim_pipe
											|| TRIM (data_source)
											|| gv_delim_pipe
											|| TRIM (other_vendor_sites)
											|| gv_delim_pipe
											|| TRIM (intro_season)
											|| gv_delim_pipe
											|| TRIM (size_run)
											|| gv_delim_pipe
											|| TRIM (pd_assigned)
											|| gv_delim_pipe
											|| TRIM (collection)
											|| gv_delim_pipe
											|| TRIM (SUBSTR (tarrif_description,1,300))
											|| gv_delim_pipe
											|| TRIM (itemcategory)
											|| gv_delim_pipe
											|| TRIM (additional_field1)
											|| gv_delim_pipe
											|| TRIM (additional_field2)
											|| gv_delim_pipe
											|| TRIM (additional_field3)
											|| gv_delim_pipe
											|| TRIM (additional_field4)
											|| gv_delim_pipe
											|| TRIM (additional_field5)
											|| gv_delim_pipe
											|| TRIM (additional_field6)
											|| gv_delim_pipe
											|| TRIM (additional_field7)
											|| gv_delim_pipe
											|| TRIM (additional_field8)
											|| gv_delim_pipe
											|| TRIM (additional_field9)
											|| gv_delim_pipe
											|| TRIM (additional_field10)
											|| gv_delim_pipe
											|| TRIM (additional_field11)
											|| gv_delim_pipe
											|| TRIM (additional_field12)
											|| gv_delim_pipe
											|| TRIM (additional_field13)
											|| gv_delim_pipe
											|| TRIM (additional_field14)
											|| gv_delim_pipe
											|| TRIM (additional_field15)
											|| gv_delim_pipe
											|| TRIM (additional_field16)
											|| gv_delim_pipe
											|| TRIM (additional_field17)
											|| gv_delim_pipe
											|| TRIM (additional_field18)
											|| gv_delim_pipe
											|| TRIM (additional_field19)
											|| gv_delim_pipe
											|| TRIM (additional_field20)
											|| gv_delim_pipe
											|| TRIM (preferential_duty_flag)
											|| gv_delim_pipe
											|| TRIM (active_flag)
											|| gv_delim_pipe
											|| TRIM (rec_status)
											|| gv_delim_pipe
											|| TRIM (error_msg)
											|| gv_delim_pipe
											|| TRIM (filename) line
						FROM (
												SELECT style_number,
																			style_description,
																			country_of_origin,
																			hts_code,
																			duty_rate,
																			additional_duty,
																			applicable_uom,
																			destination_country,
																			preferential_rate,
																			pr_start_date,
																			pr_end_date,
																			default_duty_rate,
																			default_additional_duty,
																			default_applicable_uom,
																			effective_start_date,
																			effective_end_date,
																			department,
																			data_source,
																			other_vendor_sites,
																			intro_season,
																			size_run,
																			pd_assigned,
																			collection,
																			tarrif_description,
																			itemcategory,
																			additional_field1,
																			additional_field2,
																			additional_field3,
																			additional_field4,
																			additional_field5,
																			additional_field6,
																			additional_field7,
																			additional_field8,
																			additional_field9,
																			additional_field10,
																			additional_field11,
																			additional_field12,
																			additional_field13,
																			additional_field14,
																			additional_field15,
																			additional_field16,
																			additional_field17,
																			additional_field18,
																			additional_field19,
																			additional_field20,
																			preferential_duty_flag,
																			active_flag,
																			rec_status,
																			error_msg,
																			filename 
														FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t
													WHERE 1 = 1
															AND rec_status = 'E'
															AND error_msg IS NOT NULL
															AND request_id = gn_request_id
															--AND filename = pv_file_name
										ORDER BY substr (filename, (instr (filename,'_',-1,1)+1),(instr (filename,'.',-1,1)-instr (filename,'_',-1,1)-1)) DESC);

					--DEFINE VARIABLES
					lv_output_file            UTL_FILE.file_type;
					lv_outbound_file          VARCHAR2 (32767);
					lv_err_msg                VARCHAR2 (32767) := NULL;
					lv_line                   VARCHAR2 (32767) := NULL;
					lv_directory_path         VARCHAR2 (4000);
					lv_file_name              VARCHAR2 (4000);
					l_line                    VARCHAR2 (32767);
					lv_result                 VARCHAR2 (32767);
					buffer_size               CONSTANT INTEGER := 32767;
					lv_rep_file_name_zip      VARCHAR2 (1000);


	BEGIN
 					-- lv_outbound_file := gn_request_id||'_Exception_RPT_'||TO_CHAR (SYSDATE, 'RRRR-MON-DD_HH24:MI:SS')||'.txt';   -- Commented CCR0009885
						lv_outbound_file := gn_request_id||'_Exception_RPT_'||TO_CHAR (SYSDATE, 'RRRR-MON-DD_HH24_MI_SS')||'.txt';      -- Added CCR0009885
						write_log_prc ('Exception File Name is - ' || lv_outbound_file);

						-- Derive the directory Path

						BEGIN
											SELECT directory_path
													INTO lv_directory_path
													FROM dba_directories
												WHERE 1 = 1
														AND directory_name LIKE 'XXD_CST_DUTY_ELE_EXC_DIR';
						EXCEPTION
											WHEN OTHERS
											THEN
															lv_directory_path := NULL;
						END;

						FOR i IN exception_rec
						LOOP
										l_line := i.line;
										-- write_log_prc (l_line);
						END LOOP;

						-- WRITE INTO FOLDER

						lv_output_file := UTL_FILE.fopen (lv_directory_path,
																																								lv_outbound_file,
																																								'W'       --opening the file in write mode
																																											,
																																								buffer_size);

						IF UTL_FILE.is_open (lv_output_file)
						THEN
										FOR i IN exception_hdr
										LOOP
														lv_line := i.line_hdr;
														UTL_FILE.put_line (lv_output_file, lv_line);
										END LOOP;

										FOR i IN exception_rec
										LOOP
														lv_line := i.line;
														UTL_FILE.put_line (lv_output_file, lv_line);
										END LOOP;

						ELSE
										lv_err_msg :=
														SUBSTR (
																					'Error in Opening the data file for writing. Error is : '
																		|| SQLERRM,
																		1,
																		2000);
										write_log_prc (lv_err_msg);

										RETURN;
						END IF;

						UTL_FILE.fclose (lv_output_file);						

						lv_rep_file_name_zip := substr (lv_outbound_file,1,(instr (lv_outbound_file,'.',-1)-1))||'.zip';
						write_log_prc ('Exception Report File Name is - ' || lv_outbound_file);
						write_log_prc ('Exception Report ZIP File Name is - ' || lv_rep_file_name_zip);

						create_final_zip_prc (pv_directory_name   => 'XXD_CST_DUTY_ELE_EXC_DIR',
																												pv_file_name        => lv_outbound_file,
																												pv_zip_file_name    => lv_rep_file_name_zip);

						pv_exc_file_name := lv_rep_file_name_zip;


		EXCEPTION
						WHEN UTL_FILE.invalid_path
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INVALID_PATH: File location or filename was invalid.';
										write_log_prc (lv_err_msg);
										--x_ret_code := gn_error;
										--x_ret_message := lv_err_msg;
										raise_application_error (-20101, lv_err_msg);
						WHEN UTL_FILE.invalid_mode
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INVALID_MODE: The open_mode parameter in FOPEN was invalid.';
										write_log_prc (lv_err_msg);
										--x_ret_code := gn_error;
										--x_ret_message := lv_err_msg;
										raise_application_error (-20102, lv_err_msg);
						WHEN UTL_FILE.invalid_filehandle
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INVALID_FILEHANDLE: The file handle was invalid.';
										write_log_prc (lv_err_msg);
										--x_ret_code := gn_error;
										--x_ret_message := lv_err_msg;
										raise_application_error (-20103, lv_err_msg);
						WHEN UTL_FILE.invalid_operation
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INVALID_OPERATION: The file could not be opened or operated on as requested.';
										write_log_prc (lv_err_msg);
										--x_ret_code := gn_error;
										--x_ret_message := lv_err_msg;
										raise_application_error (-20104, lv_err_msg);
						WHEN UTL_FILE.read_error
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg :=
														'READ_ERROR: An operating system error occurred during the read operation.';
										write_log_prc (lv_err_msg);
										--x_ret_code := gn_error;
										--x_ret_message := lv_err_msg;
										raise_application_error (-20105, lv_err_msg);
						WHEN UTL_FILE.write_error
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'WRITE_ERROR: An operating system error occurred during the write operation.';
										write_log_prc (lv_err_msg);
										--x_ret_code := gn_error;
										--x_ret_message := lv_err_msg;
										raise_application_error (-20106, lv_err_msg);
						WHEN UTL_FILE.internal_error
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INTERNAL_ERROR: An unspecified error in PL/SQL.';
										write_log_prc (lv_err_msg);
										--x_ret_code := gn_error;
										--x_ret_message := lv_err_msg;
										raise_application_error (-20107, lv_err_msg);
						WHEN UTL_FILE.invalid_filename
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INVALID_FILENAME: The filename parameter is invalid.';
										write_log_prc (lv_err_msg);
										--x_ret_code := gn_error;
										--x_ret_message := lv_err_msg;
										raise_application_error (-20108, lv_err_msg);
						WHEN OTHERS
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 	SUBSTR ('Error while creating or writing the data into the file.'|| SQLERRM,	1,2000);
										write_log_prc (lv_err_msg);
										--x_ret_code := gn_error;
										--x_ret_message := lv_err_msg;
										raise_application_error (-20109, lv_err_msg);

	END generate_exception_report_prc;

/***************************************************************************
	-- PROCEDURE update_item_price_prc
	-- PURPOSE: This Procedure update the price for an item
	***************************************************************************/	
	PROCEDURE update_item_price_prc (	p_item_tbl_type IN ego_item_pub.item_tbl_type)
	IS
				-- l_item_tbl_typ    ego_item_pub.item_tbl_type;
				x_item_table      ego_item_pub.item_tbl_type;
				x_message_list    error_handler.error_tbl_type;
				x_return_status   VARCHAR2 (1);
				x_msg_count       NUMBER (10);
				l_count           NUMBER;
	BEGIN
	     write_log_prc ('Procedure update_item_price_prc Begin....');

						fnd_global.apps_initialize (fnd_global.user_id,       -- User Id
																																		fnd_global.resp_id,       -- Responsibility Id
																																		fnd_global.resp_appl_id); -- Application Id


						/* l_item_tbl_typ (1).transaction_type := 'UPDATE';
							l_item_tbl_typ (1).inventory_item_id := p_inv_item_id;
							l_item_tbl_typ (1).organization_id := p_inv_org_id;
							l_item_tbl_typ (1).list_price_per_unit := p_factory_cost;*/

						ego_item_pub.process_items (p_api_version     => 1.0,
																																		p_init_msg_list   => fnd_api.g_true,
																																		p_commit          => fnd_api.g_true,
																																		p_item_tbl        => p_item_tbl_type,
																																		x_item_tbl        => x_item_table,
																																		x_return_status   => x_return_status,
																																		x_msg_count       => x_msg_count);

						IF (x_return_status <> fnd_api.g_ret_sts_success)
						THEN
										write_log_prc ('Error Messages :');
										error_handler.get_message_list (x_message_list => x_message_list);

										FOR i IN 1 .. x_message_list.COUNT
										LOOP
														write_log_prc (x_message_list (i).MESSAGE_TEXT);
										END LOOP;
						END IF;

						COMMIT;
						write_log_prc  ('Procedure update_item_price_prc Ends....'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
	EXCEPTION
						WHEN OTHERS
						THEN
										write_log_prc ('Error in update_item_price_prc Procedure -'||SQLERRM);
	END update_item_price_prc;

	/***************************************************************************
	-- PROCEDURE submit_cost_import_prc
	-- PURPOSE: This Procedure submits cost import standard program
	***************************************************************************/			
	PROCEDURE submit_cost_import_prc (p_return_mesg      OUT VARCHAR2,
																																			p_return_code      OUT VARCHAR2,
																																			p_request_id       OUT NUMBER,
																																			p_group_id      IN     NUMBER)
	IS
				l_req_id       NUMBER;
				l_phase        VARCHAR2 (100);
				l_status       VARCHAR2 (30);
				l_dev_phase    VARCHAR2 (100);
				l_dev_status   VARCHAR2 (100);
				l_wait_req     BOOLEAN;
				l_message      VARCHAR2 (2000);

	BEGIN
	     write_log_prc ('Procedure submit_cost_import_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
						l_req_id := fnd_request.submit_request (application   => 'BOM',
																																														program       => 'CSTPCIMP',
																																														argument1     => 4,
																																														-- Import Cost Option (Import item costs,resource rates, and overhead rates)
																																														argument2     => 2,
																																														-- (Mode to Run )Remove and replace cost information
																																														argument3     => 1,           -- Group Id option (specific_request_id)
																																														argument4     => NULL,        -- Dummy Group ID
																																														argument5     => p_group_id,  -- Group Id
																																														argument6     => 'AvgRates',  -- Cost Type
																																														argument7     => 2,           -- Delete Successful rows
																																														start_time    => SYSDATE,
																																														sub_request   => FALSE);
				  COMMIT;

						IF l_req_id = 0
						THEN
										p_return_code := 2;
										write_log_prc (' Unable to submit Cost Import concurrent program ');
						ELSE
										write_log_prc ('Cost Import concurrent request submitted successfully.');
										l_wait_req :=  fnd_concurrent.wait_for_request (request_id   => l_req_id,
																																																										interval     => 5,
																																																										phase        => l_phase,
																																																										status       => l_status,
																																																										dev_phase    => l_dev_phase,
																																																										dev_status   => l_dev_status,
																																																										MESSAGE      => l_message);

										IF l_dev_phase = 'COMPLETE' AND l_dev_status = 'NORMAL'
										THEN
															write_log_prc ('Cost Import concurrent request with the request id '|| l_req_id|| ' completed with NORMAL status.');
										ELSE
														p_return_code := 2;
														write_log_prc ( 'Cost Import concurrent request with the request id '|| l_req_id	|| ' did not complete with NORMAL status.');
										END IF;          -- End of if to check if the status is normal and phase is complete
						END IF;              -- End of if to check if request ID is 0.

						COMMIT;
						p_request_id := l_req_id;
						write_log_prc ('Procedure submit_cost_import_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

	EXCEPTION
						WHEN OTHERS
						THEN
										p_return_code := 2;
										p_return_mesg := 	'Error in Cost Import '
																												|| DBMS_UTILITY.format_error_stack ()
																												|| DBMS_UTILITY.format_error_backtrace ();

										write_log_prc (p_return_mesg);
										write_log_prc ('Error in submit_cost_import_prc Procedure -'||SQLERRM);
	END submit_cost_import_prc;

	/***************************************************************************
	-- PROCEDURE update_interface_status_prc
	-- PURPOSE: This Procedure update staging table rec_status based on 
	--          Interface record status
	***************************************************************************/
	PROCEDURE update_interface_status_prc (p_request_id   IN NUMBER,
																																	    			p_group_id     IN NUMBER)
	IS
			l_status         VARCHAR2 (1);
			l_err_msg        VARCHAR2 (4000);
			v_interfaced     VARCHAR2 (1);
			l_update_count   NUMBER := 0;

	  BEGIN
	       write_log_prc ('Procedure update_interface_status_prc Begins....'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

						  BEGIN
													UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t xcdeus
																SET xcdeus.rec_status = 'E'
																			,error_msg = (SELECT -- error_code||'-'||error_explanation
																			                     LISTAGG (DISTINCT int.error_explanation, '-') WITHIN GROUP (ORDER BY int.inventory_item_id)
																																		 FROM cst_item_cst_dtls_interface int
																																	 WHERE 1 = 1 
																																		 	AND TO_NUMBER (xcdeus.inventory_item_id) = int.inventory_item_id
																																			 AND TO_NUMBER (xcdeus.inventory_org_id) = int.organization_id
																																			 -- AND xcdeus.country_of_origin = int.attribute15
																																			 AND group_id = p_group_id
																																			 AND error_flag IS NOT NULL)
														WHERE EXISTS
																										(SELECT 1
																													FROM cst_item_cst_dtls_interface int
																												WHERE 1 = 1 
																														AND TO_NUMBER (xcdeus.inventory_item_id) = inventory_item_id
																														AND TO_NUMBER (xcdeus.inventory_org_id) = organization_id
																														-- AND xcdeus.country_of_origin = int.attribute15
																														AND group_id = p_group_id
																														AND error_flag IS NOT NULL)
															AND xcdeus.group_id = p_group_id
															AND xcdeus.request_id = gn_request_id
															AND xcdeus.rec_status = 'I'
															AND xcdeus.error_msg IS NULL
															AND xcdeus.active_flag = 'Y'
															AND xcdeus.coo_preference_flag = 'Y';

													write_log_prc (SQL%ROWCOUNT||' Records updated with the Rec Status as E');
													COMMIT;

													UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t u
																SET rec_status = 'E'
																			,error_msg = 'Style Failed with Oracle'
														WHERE (u.inventory_item_id,
																				 u.inventory_org_id) IN (SELECT s.inventory_item_id,
																																																				s.inventory_org_id
																																															FROM xxdo.xxd_cst_duty_ele_upld_stg_t s
																																														WHERE s.style_number = u.style_number
																																														  AND s.inventory_item_id = u.inventory_item_id
																																																AND s.inventory_org_id = u.inventory_org_id
																																																AND s.coo_preference_flag = 'Y'
																																																AND s.rec_status = 'E'
																																																AND s.group_id = p_group_id
																																																AND s.request_id = gn_request_id
																																																AND s.active_flag = 'Y'
																																																AND s.rowid = u.rowid)
																AND u.coo_preference_flag = 'N'
																AND u.rec_status = 'C'
																AND u.error_msg IS NULL
																AND u.group_id = p_group_id
																AND u.request_id = gn_request_id
																AND u.active_flag = 'Y';

													write_log_prc (SQL%ROWCOUNT||' Records updated with the Rec Status as E for other Style Numbers');
													COMMIT;

								EXCEPTION
													WHEN OTHERS
													THEN
																		write_log_prc ('Exception Occured while updating the processed status in staging table:');
								END;

								BEGIN
													-- UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t xcdeus
																-- SET xcdeus.rec_status = 'P'
														-- WHERE EXISTS
																										-- (SELECT 1
																													-- FROM cst_item_cst_dtls_interface int
																												-- WHERE 1 = 1
																														-- AND TO_NUMBER (xcdeus.inventory_item_id) = int.inventory_item_id
																														-- AND TO_NUMBER (xcdeus.inventory_org_id) = int.organization_id
																											   /*AND xcdeus.country_of_origin = int.attribute15*/
																														-- AND group_id = p_group_id
																														-- AND error_flag IS NULL
																														-- AND process_flag = 5)
																-- AND xcdeus.group_id = p_group_id
																-- AND xcdeus.request_id = gn_request_id
																-- AND xcdeus.rec_status = 'I'
																-- AND xcdeus.error_msg IS NULL
																-- AND xcdeus.active_flag = 'Y'
																-- AND xcdeus.coo_preference_flag = 'Y';

													UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t
																SET rec_status = 'P'
														WHERE rec_status = 'I'
																AND error_msg IS NULL
																AND group_id = p_group_id
																AND request_id = gn_request_id
																AND active_flag = 'Y'
																AND coo_preference_flag = 'Y';

													write_log_prc (SQL%ROWCOUNT||' Records updated with the Rec Status as P');
													COMMIT;

								EXCEPTION
													WHEN OTHERS
													THEN
																	write_log_prc ('Exception Occured while updating the processed status in staging table:');
								END;

								write_log_prc ('Procedure update_interface_status_prc Ends....'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

	EXCEPTION
						WHEN OTHERS
						THEN
										write_log_prc ('Error in update_interface_status_prc Procedure -'||SQLERRM);
	END update_interface_status_prc;

/***************************************************************************
	-- PROCEDURE insert_into_invval_duty_cost_prc
	-- PURPOSE: This Procedure insers the duty element records into xxdo_invval_duty_cost table 
	***************************************************************************/
	PROCEDURE insert_into_invval_duty_cost_prc
	IS
			CURSOR cur_data
			IS
					SELECT region,
												style_number,
												style_color,
												organization_code,
												item_number,
												country_of_origin,
												destination_country,
												duty,
												primary_duty_flag,
												duty_start_date,
												duty_end_date,
												freight,
												freight_duty,
												oh_duty,
												oh_nonduty,
												factory_cost,
												addl_duty,
												tarrif_code,
												country,
												default_category,
												operating_unit,
												inventory_org_id,
												inventory_item_id,
												coo_preference_flag
							FROM xxdo.xxd_cst_duty_ele_upld_stg_t
						WHERE 1 = 1
								AND rec_status IN ('P', 'C')
							 AND active_flag = 'Y'
								AND error_msg IS NULL
								AND request_id = gn_request_id;

					TYPE ele_data_type IS TABLE OF cur_data%ROWTYPE
																														INDEX BY BINARY_INTEGER;

					ele_data_tbl     ele_data_type;
					e_bulk_errors    EXCEPTION;
					PRAGMA EXCEPTION_INIT (e_bulk_errors, -24381);
					l_indx           NUMBER;
					l_insert_count   NUMBER := 0;
					l_update_count   NUMBER := 0;
					l_indx1          NUMBER;
					l_error_count    NUMBER;
					l_error_count1   NUMBER;
					l_msg            VARCHAR2 (4000);
					l_msg1           VARCHAR2 (4000);
					l_idx            NUMBER;
					l_idx1           NUMBER;

	BEGIN
	     write_log_prc ('Procedure insert_into_invval_duty_cost_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
						OPEN cur_data;

						ele_data_tbl.delete;

						LOOP
										FETCH cur_data
										BULK COLLECT INTO ele_data_tbl
										LIMIT g_limit;

										EXIT WHEN ele_data_tbl.COUNT = 0;

										write_log_prc ('Update the Duty Start Date, End Date if the combination of Inv Org Id, Inv Item Id, Country of Origin is already available in Duty Cost Table');

										BEGIN
															FORALL l_indx1 IN 1 .. ele_data_tbl.COUNT SAVE EXCEPTIONS

																UPDATE xxdo.xxdo_invval_duty_cost xidc
																			SET last_update_date = SYSDATE,
																							last_updated_by = gn_user_id,
																							duty_start_date = NVL (duty_start_date, TO_DATE('01-JAN-1952','DD-MON-YYYY')),
																							duty_end_date = NVL (TO_DATE(ele_data_tbl (l_indx1).duty_start_date,'YYYY-MM-DD'), TRUNC (SYSDATE)) - 1
																	WHERE inventory_org = TO_NUMBER (ele_data_tbl (l_indx1).inventory_org_id)
																			AND inventory_item_id = TO_NUMBER (ele_data_tbl (l_indx1).inventory_item_id)
																		 AND NVL (country_of_origin, -1) = NVL (ele_data_tbl (l_indx1).country_of_origin, -1) --Added for CCR0010051
																			AND NVL (duty_end_date, NVL (TO_DATE(ele_data_tbl (l_indx1).duty_start_date,'YYYY-MM-DD'),	TRUNC (SYSDATE))) >= 	
																							NVL (TO_DATE(ele_data_tbl (l_indx1).duty_start_date,'YYYY-MM-DD'),	TRUNC (SYSDATE));

																l_update_count := l_update_count + SQL%ROWCOUNT;

										EXCEPTION
															WHEN e_bulk_errors
															THEN
																	write_log_prc  ('Inside E_BULK_ERRORS');
																	l_error_count1 := SQL%BULK_EXCEPTIONS.COUNT;

																	FOR i IN 1 .. l_error_count1
																	LOOP
																					l_msg1 := SQLERRM (-SQL%BULK_EXCEPTIONS (i).ERROR_CODE);
																					l_idx1 := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
																					write_log_prc ( 'Failed to update item -'|| ele_data_tbl (l_idx1).inventory_item_id|| ' for org - '
																																							|| ele_data_tbl (l_idx1).inventory_org_id
																																							|| ' with error_code- '|| l_msg1);
																	END LOOP;

															WHEN OTHERS
															THEN
																	write_log_prc ( 'Inside Others for header update. ');
										END;
						END LOOP;

						CLOSE cur_data;

						write_log_prc ('succesfully updated '|| l_update_count|| ' records into table xxdo_invval_duty_cost');

						COMMIT;

						OPEN cur_data;

						ele_data_tbl.delete;

						LOOP
										FETCH cur_data
										BULK COLLECT INTO ele_data_tbl
										LIMIT g_limit;

										EXIT WHEN ele_data_tbl.COUNT = 0;

          write_log_prc ('Inserting Data into xxdo.xxdo_invval_duty_cost table');
										BEGIN
															FORALL l_indx IN 1 .. ele_data_tbl.COUNT SAVE EXCEPTIONS
															INSERT INTO xxdo.xxdo_invval_duty_cost (
																																																							operating_unit,
																																																							country_of_origin,
																																																							primary_duty_flag,
																																																							oh_duty,
																																																							oh_nonduty,
																																																							freight,
																																																							freight_duty,
																																																							additional_duty,
																																																							inventory_org,
																																																							inventory_item_id,
																																																							duty,
																																																							duty_start_date,
																																																							duty_end_date,
																																																							style_color,
																																																							--rec_status,
																																																							--error_msg,
																																																							--request_id,
																																																							--rec_identifier,
																																																							last_update_date,
																																																							last_updated_by,
																																																							creation_date,
																																																							created_by
																																																							)
																																																	VALUES 
																																																							(
																																																							ele_data_tbl (l_indx).operating_unit,
																																																							ele_data_tbl (l_indx).country_of_origin,
																																																							ele_data_tbl (l_indx).primary_duty_flag,
																																																							ele_data_tbl (l_indx).oh_duty,
																																																							ele_data_tbl (l_indx).oh_nonduty,
																																																							ele_data_tbl (l_indx).freight,
																																																							ele_data_tbl (l_indx).freight_duty,
																																																							ele_data_tbl (l_indx).addl_duty,
																																																							ele_data_tbl (l_indx).inventory_org_id,
																																																							ele_data_tbl (l_indx).inventory_item_id,
																																																							ele_data_tbl (l_indx).duty,
																																																							TO_DATE (ele_data_tbl (l_indx).duty_start_date, 'YYYY-MM-DD'),
																																																							DECODE (ele_data_tbl (l_indx).coo_preference_flag, 'Y', NULL, TO_DATE (ele_data_tbl (l_indx).duty_end_date, 'YYYY-MM-DD')),
																																																							ele_data_tbl (l_indx).style_color,
																																																							--'N',
																																																							--NULL,
																																																							--gn_request_id,
																																																							--xxdo.xxdo_invval_duty_cost.NEXTVAL,
																																																							SYSDATE,
																																																							gn_user_id,
																																																							SYSDATE,
																																																							gn_user_id
																																																							);

															l_insert_count := l_insert_count + SQL%ROWCOUNT;

										EXCEPTION
															WHEN e_bulk_errors
															THEN
																			write_log_prc ('Inside E_BULK_ERRORS');
																			l_error_count := SQL%BULK_EXCEPTIONS.COUNT;

																			FOR i IN 1 .. l_error_count
																			LOOP
																							l_msg := SQLERRM (-SQL%BULK_EXCEPTIONS (i).ERROR_CODE);
																							l_idx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
																							write_log_prc ('Failed to insert item -'|| ele_data_tbl (l_idx).inventory_item_id|| ' for org - '
																																									|| ele_data_tbl (l_idx).inventory_org_id	|| ' with error_code- '	|| l_msg);

																			END LOOP;

																			COMMIT;

															WHEN OTHERS
															THEN
																			write_log_prc ('Inside Others for header insert. ');
										END;

						END LOOP;

						CLOSE cur_data;

						COMMIT;

						g_invval_cnt := l_insert_count;

						write_log_prc ( 'succesfully inserted ' || l_insert_count|| ' records into table xxdo_invval_duty_cost');
					 write_log_prc ('Procedure insert_into_invval_duty_cost_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

	EXCEPTION
						WHEN OTHERS
						THEN
										--retcode := 2;
										write_log_prc ('Error in Procedure insert_into_invval_duty_cost_prc:' || SQLERRM);

	END insert_into_invval_duty_cost_prc;

	/***************************************************************************
	-- PROCEDURE write_duty_ele_report_prc
	-- PURPOSE: This Procedure generates the duty elements report
	***************************************************************************/

 PROCEDURE write_duty_ele_report_prc (pv_region       IN    VARCHAR2, 
																                      pv_inv_org      IN    VARCHAR2, 
																																						pv_style        IN    VARCHAR2, 
																																						pv_color        IN    VARCHAR2,
	                                     pv_mode         IN    VARCHAR2,
																																						pv_dis_sku      IN    VARCHAR2,
																																						pv_send_mail    IN    VARCHAR2,
																																						pv_rep_filename   OUT VARCHAR2)
	IS
	  CURSOR duty_ele_rep
			IS   
					SELECT style_number,
												style_number||'-'||style_color style_color,
												item_size,
												organization_code,
												item_number,
												country_of_origin,
												duty,
												primary_duty_flag,
												duty_start_date,
												duty_end_date,
												freight,
												freight_duty,
												oh_duty,
												oh_nonduty,
												factory_cost,
												addl_duty,
												tarrif_code,
												country,
												default_category
							FROM xxdo.xxd_cst_duty_ele_upld_stg_t
						WHERE 1 = 1
								AND region = NVL (pv_region, region)
								AND inventory_org_id = NVL (pv_inv_org, inventory_org_id)
								AND style_number = NVL (pv_style, style_number)
								AND style_color = NVL (pv_color, style_color)
								AND rec_status = DECODE (NVL (pv_mode,'Preview'), 'Preview', 'N', 'Process', 'P')
							 AND active_flag = 'Y'
								AND request_id = gn_request_id
			ORDER BY decode (NVL (pv_dis_sku,'N'),'N', style_number), 
			                                      'Y', style_number, style_color, item_size;

			lv_file_path       VARCHAR2 (100);
			lv_hdr_line        VARCHAR2 (1000);
			lv_line            VARCHAR2 (32000);
			lv_output_file     UTL_FILE.file_type;
			lv_outbound_file   VARCHAR2 (1000);
			x_ret_code         VARCHAR2 (100);
			lv_err_msg         VARCHAR2 (100);
			x_ret_message      VARCHAR2 (100);
			lv_header          VARCHAR2 (1) := 'Y';
			buffer_size   CONSTANT INTEGER := 32767;

		BEGIN
		     write_log_prc ('Procedure write_duty_ele_report_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));							
							lv_outbound_file := gn_request_id||'_Costing_Material_Overhead_Elements_Upload.csv';

								-- Derive the directory Path

								BEGIN
													SELECT directory_path
															INTO lv_file_path
															FROM dba_directories
														WHERE 1 = 1
																AND directory_name LIKE 'XXD_CST_DUTY_ELE_REP_DIR';
								EXCEPTION
													WHEN OTHERS
													THEN
																	lv_file_path := NULL;
								END;

							lv_hdr_line := 'Style'
																						||gv_delim_comma
																						||'Style Color'
																						||gv_delim_comma
																						||'Organization Code'
																						||gv_delim_comma
																						||'Item'
																						||gv_delim_comma
																						||'Country Of Origin'
																						||gv_delim_comma
																						||'Duty'
																						||gv_delim_comma
																						||'Prime Duty'
																						||gv_delim_comma
																						||'Duty Start Date'
																						||gv_delim_comma
																						||'Duty End Date'
																						||gv_delim_comma
																						||'Freight'
																						||gv_delim_comma
																						||'Freight Duty'
																						||gv_delim_comma
																						||'OH Duty'
																						||gv_delim_comma
																						||'OH Nonduty'
																						||gv_delim_comma
																						||'Factory Cost'
																						||gv_delim_comma
																						||'Add''l Duty'
																						||gv_delim_comma
																						||'Tariff Code'
																						||gv_delim_comma
																						||'Country'
																						||gv_delim_comma
																						||'Default Category';

									 -- FOR i IN duty_ele_rep
					     -- LOOP
									    --  l_line := i.line;
									  -- write_log_prc (l_line);
					     -- END LOOP;

							-- WRITE INTO FOLDER
							write_log_prc  ('Duty Elements File Name is - ' || lv_outbound_file);

							lv_output_file := UTL_FILE.fopen (lv_file_path,
																																									lv_outbound_file,
																																									'W'       --opening the file in write mode
																																												,
																																									buffer_size);

							IF UTL_FILE.is_open (lv_output_file)
							THEN
											IF lv_header = 'Y'
											THEN
															lv_line := lv_hdr_line;
															UTL_FILE.put_line (lv_output_file, lv_line);
											END IF;

											FOR i IN duty_ele_rep
											LOOP
															lv_line :=   i.style_number
																										||gv_delim_comma
																										||i.style_color
																										||gv_delim_comma
																										||i.organization_code
																										||gv_delim_comma
																										||i.item_number
																										||gv_delim_comma
																										||i.country_of_origin
																										||gv_delim_comma
																										||i.duty
																										||gv_delim_comma
																										||i.primary_duty_flag
																										||gv_delim_comma
																										||i.duty_start_date
																										||gv_delim_comma
																										||i.duty_end_date
																										||gv_delim_comma
																										||i.freight
																										||gv_delim_comma
																										||i.freight_duty
																										||gv_delim_comma
																										||i.oh_duty
																										||gv_delim_comma
																										||i.oh_nonduty
																										||gv_delim_comma
																										||i.factory_cost
																										||gv_delim_comma
																										||i.addl_duty
																										||gv_delim_comma
																										||i.tarrif_code
																										||gv_delim_comma
																										||i.country
																										||gv_delim_comma
																										||i.default_category;
															UTL_FILE.put_line (lv_output_file, lv_line);
											END LOOP;
							ELSE
											lv_err_msg := SUBSTR ('Error in Opening the Duty Elements data file for writing. Error is : '|| SQLERRM,1,	2000);
											write_log_prc (lv_err_msg);
											x_ret_code := gn_error;
											x_ret_message := lv_err_msg;
											RETURN;
							END IF;

							UTL_FILE.fclose (lv_output_file);

							pv_rep_filename := lv_outbound_file;

				  	write_log_prc ('Procedure write_duty_ele_report_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

	EXCEPTION
						WHEN UTL_FILE.invalid_path
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INVALID_PATH: File location or filename was invalid.';
										write_log_prc (lv_err_msg);
										x_ret_code := gn_error;
										x_ret_message := lv_err_msg;
										raise_application_error (-20101, lv_err_msg);

						WHEN UTL_FILE.invalid_mode
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INVALID_MODE: The open_mode parameter in FOPEN was invalid.';
										write_log_prc (lv_err_msg);
										x_ret_code := gn_error;
										x_ret_message := lv_err_msg;
										raise_application_error (-20102, lv_err_msg);

						WHEN UTL_FILE.invalid_filehandle
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INVALID_FILEHANDLE: The file handle was invalid.';
										write_log_prc (lv_err_msg);
										x_ret_code := gn_error;
										x_ret_message := lv_err_msg;
										raise_application_error (-20103, lv_err_msg);

						WHEN UTL_FILE.invalid_operation
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INVALID_OPERATION: The file could not be opened or operated on as requested.';
										write_log_prc (lv_err_msg);
										x_ret_code := gn_error;
										x_ret_message := lv_err_msg;
										raise_application_error (-20104, lv_err_msg);

						WHEN UTL_FILE.read_error
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'READ_ERROR: An operating system error occurred during the read operation.';
										write_log_prc (lv_err_msg);
										x_ret_code := gn_error;
										x_ret_message := lv_err_msg;
										raise_application_error (-20105, lv_err_msg);

						WHEN UTL_FILE.write_error
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'WRITE_ERROR: An operating system error occurred during the write operation.';
										write_log_prc (lv_err_msg);
										x_ret_code := gn_error;
										x_ret_message := lv_err_msg;
										raise_application_error (-20106, lv_err_msg);

						WHEN UTL_FILE.internal_error
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INTERNAL_ERROR: An unspecified error in PL/SQL.';
										write_log_prc (lv_err_msg);
										x_ret_code := gn_error;
										x_ret_message := lv_err_msg;
										raise_application_error (-20107, lv_err_msg);

						WHEN UTL_FILE.invalid_filename
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := 'INVALID_FILENAME: The filename parameter is invalid.';
										write_log_prc (lv_err_msg);
										x_ret_code := gn_error;
										x_ret_message := lv_err_msg;
										raise_application_error (-20108, lv_err_msg);

						WHEN OTHERS
						THEN
										IF UTL_FILE.is_open (lv_output_file)
										THEN
														UTL_FILE.fclose (lv_output_file);
										END IF;

										lv_err_msg := SUBSTR (	'Error while creating or writing the data into the file.'	|| SQLERRM,1,2000);
										write_log_prc (lv_err_msg);
										x_ret_code := gn_error;
										x_ret_message := lv_err_msg;
										raise_application_error (-20109, lv_err_msg);

 END write_duty_ele_report_prc;

	/***************************************************************************
	-- PROCEDURE duty_ele_rep_send_mail_prc
	-- PURPOSE: This Procedure sends duty element report to the PD Team
	***************************************************************************/
PROCEDURE duty_ele_rep_send_mail_prc (pv_rep_file_name IN VARCHAR2,
                                      pv_err_code         OUT VARCHAR2)
IS
  lv_rep_file_name       VARCHAR2 (4000);
		lv_rep_file_name_zip   VARCHAR2 (4000);
		lv_message             VARCHAR2 (4000);
		lv_directory_path      VARCHAR2 (100);
		-- lv_gl_directory_path   VARCHAR2 (100);
		lv_mail_delimiter      VARCHAR2 (1):='/';
		lv_recipients          VARCHAR2 (1000);
		lv_ccrecipients        VARCHAR2 (1000);
		lv_result              VARCHAR2 (4000);
		lv_result_msg          VARCHAR2 (4000);
		lv_message1            VARCHAR2 (32000);
		lv_message2            VARCHAR2 (32000);
		lv_message3            VARCHAR2 (32000);

		CURSOR c_write_errors
		IS
				--SELECT LISTAGG(distinct style_number,',' on overflow truncate with count) WITHIN GROUP (ORDER BY style_number) style_number, error_msg, COUNT(1) err_cnt
    SELECT error_msg, COUNT(1) err_cnt
						FROM xxdo.xxd_cst_duty_ele_upld_stg_t
					WHERE rec_status = 'E'
							AND error_msg IS NOT NULL
						-- AND group_id = p_group_id
							AND request_id = gn_request_id
		GROUP BY error_msg;

  BEGIN
		     write_log_prc ('Procedure duty_ele_rep_send_mail_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

							-- Derive the directory Path

							BEGIN
												SELECT directory_path
														INTO lv_directory_path
														FROM dba_directories
													WHERE 1 = 1
															AND directory_name LIKE 'XXD_CST_DUTY_ELE_REP_DIR';
							EXCEPTION
												WHEN OTHERS
												THEN
																lv_directory_path := NULL;
							END;

							lv_rep_file_name := lv_directory_path||lv_mail_delimiter||pv_rep_file_name;
							lv_rep_file_name_zip := substr (pv_rep_file_name,1,(instr (pv_rep_file_name,'.',-1)-1))||'.zip';
							write_log_prc ('Duty Elements Report File Name is - ' || lv_rep_file_name);
							write_log_prc ('Duty Elements Report ZIP File Name is - ' || lv_rep_file_name_zip);

							create_final_zip_prc (pv_directory_name   => 'XXD_CST_DUTY_ELE_REP_DIR',
																													pv_file_name        => pv_rep_file_name,
																													pv_zip_file_name    => lv_rep_file_name_zip);

							lv_rep_file_name_zip := lv_directory_path||lv_mail_delimiter||lv_rep_file_name_zip;

							lv_message2 := CHR(10)||'Distinct Error Messages :'||
							               CHR(10)||'========================='||
																						CHR(10)||'Count'||CHR(9)||'Error Message'||
							               CHR(10)||'-----------------------------------------------------------------';

							FOR i in c_write_errors
							LOOP
											lv_message3 := CASE WHEN lv_message3 IS NOT NULL
																										THEN
																														lv_message3||CHR(10)||i.err_cnt||CHR(9)||i.error_msg
																										ELSE 
																														i.err_cnt||CHR(9)||i.error_msg
																										END;
							END LOOP;

							lv_message3 := SUBSTR (lv_message3,1,30000);

							lv_message1 := 'Hello Team,'||CHR(10)||CHR(10)||'Please Find the Attached Deckers Costing Material Overhead Elements Report. '||
							CHR(10)||CHR(10)||lv_message2||CHR(10)||lv_message3||CHR(10)||CHR(10)||'Regards,'||CHR(10)||'SYSADMIN.'|| CHR (10)|| CHR (10)|| 'Note: This is auto generated mail, please donot reply.';

							SELECT LISTAGG (ffvl.description, ';') WITHIN GROUP (ORDER BY ffvl.description)
									INTO lv_recipients
									FROM apps.fnd_flex_value_sets  fvs,
														apps.fnd_flex_values_vl   ffvl
								WHERE 1 = 1
								  AND fvs.flex_value_set_id = ffvl.flex_value_set_id
										AND fvs.flex_value_set_name = 'XXD_TRO_EMAIL_TO_PD_VS'
										AND NVL (TRUNC (ffvl.start_date_active),TRUNC (SYSDATE)) <=TRUNC (SYSDATE)
										AND NVL (TRUNC (ffvl.end_date_active),TRUNC (SYSDATE)) >=TRUNC (SYSDATE)
										AND ffvl.enabled_flag = 'Y';

										xxdo_mail_pkg.send_mail (pv_sender       => 'erp@deckers.com',
																																			pv_recipients   => lv_recipients,
																																			pv_ccrecipients => lv_ccrecipients,
																																			pv_subject      => 'Deckers Costing Material Overhead Elements Report',
																																			pv_message      => lv_message1,
																																			pv_attachments  => lv_rep_file_name_zip,
																																			xv_result       => lv_result,
																																			xv_result_msg   => lv_result_msg);

										write_log_prc (lv_result);
										write_log_prc (lv_result_msg);
										write_log_prc ('Procedure duty_ele_rep_send_mail_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

	EXCEPTION
	     WHEN OTHERS
						THEN
						    pv_err_code := 2;
						    write_log_prc ('Error in Procedure duty_ele_rep_send_mail_prc:' || SQLERRM);
 END duty_ele_rep_send_mail_prc;

	/***************************************************************************
	-- PROCEDURE insert_into_interface_prc
	-- PURPOSE: This Procedure inserts the duty element records to CST Interface tables
	***************************************************************************/
	PROCEDURE insert_into_interface_prc (errbuf     OUT   VARCHAR2,
																																	    	retcode    OUT   VARCHAR2,
                                      pv_region     IN VARCHAR2, 
																																						pv_inv_org    IN VARCHAR2, 
																																						pv_style      IN VARCHAR2, 
																																						pv_color      IN VARCHAR2, 
																																						pv_reprocess  IN VARCHAR2,
																																						pv_mode       IN VARCHAR2,
																																						pv_send_mail  IN VARCHAR2,
																																						pv_dis_sku    IN VARCHAR2)
	IS
			CURSOR c_subelements
			IS
					SELECT rowid,u.*
							FROM xxdo.xxd_cst_duty_ele_upld_stg_t u
						WHERE 1 = 1
						  AND u.region = NVL (pv_region, u.region)
								AND u.inventory_org_id = NVL (pv_inv_org, u.inventory_org_id)
								AND u.style_number = NVL (pv_style, u.style_number)
								AND u.style_color = NVL (pv_color, u.style_color)
						  AND u.rec_status = 'N'
							 AND u.active_flag = 'Y'
								AND u.request_id = gn_request_id;

			CURSOR c_group_id
			IS
			  SELECT DISTINCT GROUP_ID
					  FROM xxdo.xxd_cst_duty_ele_upld_stg_t
						WHERE 1 = 1
						  AND region = NVL (pv_region, region)
								AND inventory_org_id = NVL (pv_inv_org, inventory_org_id)
								AND style_number = NVL (pv_style, style_number)
								AND style_color = NVL (pv_color, style_color)
						  AND rec_status = 'I'
							 AND active_flag = 'Y'
								AND request_id = gn_request_id;

			CURSOR c_write_errors
			IS
					SELECT error_msg, COUNT(1) err_cnt
					  FROM xxdo.xxd_cst_duty_ele_upld_stg_t
						WHERE rec_status = 'E'
						  AND error_msg IS NOT NULL
						  AND request_id = gn_request_id        
			GROUP BY error_msg;

   TYPE tb_rec IS TABLE OF c_subelements%ROWTYPE
			INDEX BY BINARY_INTEGER;

			v_tb_rec      tb_rec;

			v_bulk_limit              NUMBER := 5000;
   e_bulk_errors             EXCEPTION;
   PRAGMA EXCEPTION_INIT     (e_bulk_errors, -24381);
			l_msg                     VARCHAR2 (4000);
   l_idx                     NUMBER;
			l_error_count             NUMBER;

			l_group_id            			NUMBER;
			l_price               			NUMBER;
			l_cost_err_msg        			VARCHAR2 (4000);
			l_cost_err_code       			VARCHAR2 (4000);
			l_err_msg             			VARCHAR2 (4000);
			l_duty_basis          			NUMBER;
			l_freight_basis       			NUMBER;
			l_oh_duty_basis       			NUMBER;
			l_oh_nonduty_basis    			NUMBER;
			l_freight_du_basis    			NUMBER;
			l_int_req_id          			NUMBER;
			p_errbuff             			VARCHAR2 (4000);
			p_retcode             			VARCHAR2 (4000);
			-- l_no_item             			NUMBER;
			-- l_int_err             			NUMBER;
			-- l_processed           			NUMBER;
			-- l_total               			NUMBER;
			v_interfaced          			VARCHAR2 (1);
			v_processed           			VARCHAR2 (1);
			-- l_validate_err_msg    			VARCHAR2 (4000);
			user_exception        			EXCEPTION;
			l_insert_count        			NUMBER := 0;
			-- l_duplicate_records   			NUMBER := 0;
			l_item_tbl_typ        			ego_item_pub.item_tbl_type;
			api_index             			NUMBER := 0;
			lv_rep_file_name      			VARCHAR2 (100);
			ln_rec_total          			NUMBER := 0;
			ln_rec_fail           			NUMBER := 0;
			ln_rec_success        			NUMBER := 0;
			lv_err_code           			VARCHAR2 (1);
			lv_style_numbers      			VARCHAR2 (4000);
			lv_tot_addl_duty_per     VARCHAR2 (240);
			lv_duty_rate             VARCHAR2 (240);
			lv_start_date            VARCHAR2 (240);
			lv_end_date              VARCHAR2 (240);
			lv_sku_weight_uom_code   VARCHAR2 (240);
		 lv_sku_unit_weight       VARCHAR2 (240);
			ln_ele_rec_total         NUMBER := 0;
			ln_ele_rec_success       NUMBER := 0;
			ln_rec_oracle            NUMBER := 0;
			ln_rec_success_oracle    NUMBER := 0;
			ln_rec_fail_oracle       NUMBER := 0;
			ln_ele_rec_error         NUMBER := 0;
			ln_tot_cnt               NUMBER := 0;
			ln_cnt                   NUMBER := 0;
			ln_inb_cnt               NUMBER := 0;
			ln_upd_cnt               NUMBER := 0;
			ln_rec_na                NUMBER := 0;
			lv_duty                  VARCHAR2 (1000);
			lv_freight               VARCHAR2 (1000);
			lv_oh_duty               VARCHAR2 (1000);
			lv_oh_nonduty            VARCHAR2 (1000);
			lv_freight_duty          VARCHAR2 (1000);

			BEGIN
								write_log_prc ('Procedure insert_into_interface_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
								write_log_prc ('Region-'||pv_region||'Inventory Org-'||pv_inv_org||'Style-'||pv_style||'Color-'||pv_color||'Reprocess-'||pv_reprocess||'Mode-'||pv_mode||'Send Mail-'||pv_send_mail||'Display SKU-'||pv_dis_sku);

								apps.fnd_file.put_line(apps.fnd_file.output,'                                                                               Deckers CM Update and Upload Cost Elements-Sku Level ');
								apps.fnd_file.put_line(apps.fnd_file.output,'');
								apps.fnd_file.put_line(apps.fnd_file.output,'Date:'||TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
								apps.fnd_file.put_line(apps.fnd_file.output,'');
								apps.fnd_file.put_line(apps.fnd_file.output,'');
								apps.fnd_file.put_line(apps.fnd_file.output,'Parameters: ');
								apps.fnd_file.put_line(apps.fnd_file.output,'------------');
								apps.fnd_file.put_line(apps.fnd_file.output,'Region: '||pv_region);
								apps.fnd_file.put_line(apps.fnd_file.output,'Inventory Organization: '||pv_inv_org);
								apps.fnd_file.put_line(apps.fnd_file.output,'Style: '||pv_style);
								apps.fnd_file.put_line(apps.fnd_file.output,'Style Color: '||pv_color);
								apps.fnd_file.put_line(apps.fnd_file.output,'Re-Process: '||pv_reprocess);
								apps.fnd_file.put_line(apps.fnd_file.output,'Mode: '||pv_mode);
								apps.fnd_file.put_line(apps.fnd_file.output,'Send Mail: '||pv_send_mail);
								apps.fnd_file.put_line(apps.fnd_file.output,'Display SKU:'||pv_dis_sku);
								apps.fnd_file.put_line(apps.fnd_file.output,'');
								apps.fnd_file.put_line(apps.fnd_file.output,'');

/*							 insert_into_custom_table_prc;

								BEGIN
													SELECT COUNT(1)
													  INTO ln_tot_cnt
															FROM xxdo.xxd_cst_duty_ele_upld_stg_t
														WHERE 1 = 1
																AND rec_status = 'N'
																AND error_msg IS NULL
																AND active_flag = 'Y';

													apps.fnd_file.put_line(apps.fnd_file.output,'*************************************************************************************************');
													apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Successfully Inserted/Exists in Process Staging Table        - '||ln_tot_cnt);

								EXCEPTION
													WHEN Others
													THEN
																	write_log_prc ('Exception occurred while retriving the count from xxdo.xxd_cst_duty_ele_inb_stg_tr_t');
																	ln_cnt := 0;
								END;*/

								IF NVL (pv_reprocess, 'N') = 'Y'
								THEN

												write_log_prc ('Update Error Records Flag as N');

												UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t
															SET rec_status = 'N'
																		,error_msg = NULL
																		,request_id = gn_request_id
																		,created_by = gn_user_id
																		,creation_date = SYSDATE
																		,last_updated_by = gn_user_id
																		,last_update_date = SYSDATE
													WHERE rec_status = 'E'
															AND error_msg IS NOT NULL
															AND active_flag = 'Y'
															AND region = NVL (pv_region, region)
															AND inventory_org_id = NVL (pv_inv_org, inventory_org_id)
															AND style_number = NVL (pv_style, style_number)
															AND style_color = NVL (pv_color, style_color);

												write_log_prc (SQL%ROWCOUNT||' Error Records Updated with Flag N');
												COMMIT;

												-- write_log_prc ('Records Count ');

												-- SELECT COUNT(1)
														-- INTO ln_ele_rec_total
														-- FROM xxdo.xxd_cst_duty_ele_upld_stg_t
													-- WHERE rec_status = 'N'
															-- AND active_flag = 'Y'
															-- AND request_id = gn_request_id;

												-- apps.fnd_file.put_line(apps.fnd_file.output,'**************************************************************************************');
												-- apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows in Process Staging Table  -                         '||ln_ele_rec_total);


								ELSIF NVL (pv_reprocess, 'N') = 'N'
								THEN

											write_log_prc ('Update Request ID and WHO columns for unprocessed records in stg table...');

											UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t
														SET request_id = gn_request_id
																	,created_by = gn_user_id
																	,creation_date = SYSDATE
																	,last_updated_by = gn_user_id
																	,last_update_date = SYSDATE
												WHERE rec_status = 'N'
														AND error_msg IS NULL
														AND active_flag = 'Y'
														AND region = NVL (pv_region, region)
														AND inventory_org_id = NVL (pv_inv_org, inventory_org_id)
														AND style_number = NVL (pv_style, style_number)
														AND style_color = NVL (pv_color, style_color);

											write_log_prc (SQL%ROWCOUNT||' Records Updated with Request ID and WHO Columns');
											COMMIT;

											-- BEGIN
											     -- SELECT COUNT(1)
																  -- INTO ln_inb_cnt
																		-- FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t
																	-- WHERE 1 = 1
																			-- AND rec_status = 'V'
																			-- AND active_flag = 'Y'
																			-- AND error_msg IS NULL;

											-- EXCEPTION
											     -- WHEN OTHERS
																-- THEN
																    -- ln_inb_cnt := 0;
											-- END;

											-- IF ln_inb_cnt > 0
											-- THEN																	

											    -- insert_into_custom_table_prc;


											-- ELSIF ln_inb_cnt = 0
											-- THEN
											    -- BEGIN
																				-- SELECT COUNT(1)
																						-- INTO ln_upd_cnt
																						-- FROM xxdo.xxd_cst_duty_ele_upld_stg_t
																					-- WHERE rec_status = 'N'
																							-- AND error_msg IS NULL
																							-- AND active_flag = 'Y'
																							-- AND request_id = gn_request_id;
															-- EXCEPTION
																				-- WHEN OTHERS
																				-- THEN
																								-- ln_upd_cnt := 0;
															-- END;

															-- IF ln_upd_cnt > 0
															-- THEN
											       -- apps.fnd_file.put_line(apps.fnd_file.output,'**************************************************************************************');
											       -- apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows in Process Staging Table  -                         '||ln_upd_cnt);
															-- END IF;

											-- END IF;

								END IF;

								BEGIN
													SELECT COUNT(1)
													  INTO ln_cnt
															FROM xxdo.xxd_cst_duty_ele_upld_stg_t
														WHERE 1 = 1
																AND rec_status = 'N'
																AND error_msg IS NULL
																AND active_flag = 'Y'
																AND request_id = gn_request_id;
								EXCEPTION
													WHEN Others
													THEN
																	write_log_prc ('Exception occurred while retriving the count from xxdo.xxd_cst_duty_ele_upld_stg_t');
																	ln_cnt := 0;
								END;

        apps.fnd_file.put_line(apps.fnd_file.output,'*************************************************************************************************');
								apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Considered to Process Based on Parameters                    - '||ln_cnt);

								IF ln_cnt > 0
								THEN

												write_log_prc ('Duty Calculation Process Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

												OPEN c_subelements;
												v_tb_rec.DELETE;

																	LOOP
																					FETCH c_subelements
																											BULK COLLECT INTO v_tb_rec
																											LIMIT v_bulk_limit;

																					EXIT WHEN v_tb_rec.COUNT = 0;

																					IF v_tb_rec.COUNT > 0
																					THEN
																									write_log_prc ('Record Count: '||v_tb_rec.COUNT);
																								 BEGIN

																													 FOR i IN 1 .. v_tb_rec.COUNT
																													 LOOP

																                  IF v_tb_rec(i).preferential_rate = 'Y' AND v_tb_rec(i).duty_rate IS NOT NULL OR 
																																		   v_tb_rec(i).preferential_rate = 'N' AND v_tb_rec(i).default_duty_rate IS NOT NULL
																																		THEN

																																						IF (v_tb_rec(i).preferential_rate = 'Y' AND v_tb_rec(i).additional_duty IS NOT NULL AND v_tb_rec(i).applicable_uom IS NOT NULL) OR 
																																									(v_tb_rec(i).preferential_rate = 'N' AND v_tb_rec(i).default_additional_duty IS NOT NULL AND v_tb_rec(i).default_applicable_uom IS NOT NULL)
																																						THEN

																																										BEGIN
																																															SELECT weight_uom_code
																																																					,unit_weight
																																																	INTO lv_sku_weight_uom_code
																																																					,lv_sku_unit_weight
																																																	FROM mtl_system_items_b
																																																WHERE inventory_item_id = v_tb_rec(i).inventory_item_id
																																																		AND organization_id = v_tb_rec(i).inventory_org_id;

																																															-- write_log_prc ('UOM Values from Oracle '||lv_sku_weight_uom_code||'-'||lv_sku_unit_weight);

																																										EXCEPTION
																																															WHEN OTHERS
																																															THEN
																																																			v_tb_rec(i).rec_status := 'E';
																																																			v_tb_rec(i).error_msg := 'Retiveing UOM Values Failed-';
																																																			apps.fnd_file.put_line(apps.fnd_file.log, ' Retiveing UOM Values Failed for the Inv Item ID and  Inv Org ID :'||v_tb_rec(i).inventory_item_id||'-'||v_tb_rec(i).inventory_org_id);
																																																			-- dbms_output.put_line (' Retiveing UOM Values Failed for the Inv Item ID and Inv Org ID :'||v_tb_rec(i).inventory_item_id||'-'||v_tb_rec(i).inventory_org_id);
																																																			lv_sku_weight_uom_code := NULL;
																																																			lv_sku_unit_weight := NULL;

																																										END;

																																										IF lv_sku_weight_uom_code IS NOT NULL AND lv_sku_unit_weight IS NOT NULL
																																										THEN

																																														BEGIN	
																																																			-- write_log_prc (' UOM Conversion ');
																																																			-- write_log_prc ('Inv Item Id :'||v_tb_rec(i).inventory_item_id||'Inv Org Id :'||v_tb_rec(i).inventory_org_id||'Additional Duty :'||v_tb_rec(i).additional_duty||'Applicable UOM :'||v_tb_rec(i).applicable_uom);
																																																			lv_tot_addl_duty_per := NULL;

																																																			lv_tot_addl_duty_per := duty_uom_conversion_fnc (v_tb_rec(i).inventory_item_id
																																																																																																			,v_tb_rec(i).inventory_org_id
																																																																																																			,v_tb_rec(i).additional_duty
																																																																																																			,v_tb_rec(i).applicable_uom
																																																																																																			,v_tb_rec(i).preferential_rate
																																																																																																			,v_tb_rec(i).preferential_duty_flag
																																																																																																			,v_tb_rec(i).default_additional_duty
																																																																																																			,v_tb_rec(i).default_applicable_uom);

																																																			-- write_log_prc ('Total Additional Duty Percentage :'||lv_tot_addl_duty_per);

																																														EXCEPTION
																																																			WHEN OTHERS 
																																																			THEN
																																																							v_tb_rec(i).rec_status := 'E';
																																																							v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'Exception Occurred for calcluating Additional Duty Percentage-';
																																																							write_log_prc (' Exception Occurred while calcluating Additional Duty Percentage');
																																																							lv_tot_addl_duty_per := NULL;
																																														END;

																																										ELSE
																																														v_tb_rec(i).rec_status := 'E';
																																														v_tb_rec(i).error_msg := v_tb_rec(i).error_msg||'WEIGHT_UOM_CODE OR UNIT_WEIGHT Not Available in Oracle-';

																																										END IF;

																																						END IF;

																																						IF v_tb_rec(i).preferential_rate = 'Y'
																																						THEN

																																										v_tb_rec(i).duty := v_tb_rec(i).duty_rate + NVL (lv_tot_addl_duty_per,0);
																																										v_tb_rec(i).duty_start_date := v_tb_rec(i).pr_start_date;
																																										v_tb_rec(i).duty_end_date := v_tb_rec(i).pr_end_date;

																																						ELSIF v_tb_rec(i).preferential_rate = 'N'
																																						THEN

																																										v_tb_rec(i).duty := v_tb_rec(i).default_duty_rate + NVL (lv_tot_addl_duty_per,0);
																																										v_tb_rec(i).duty_start_date := v_tb_rec(i).effective_start_date;
																																										v_tb_rec(i).duty_end_date := v_tb_rec(i).effective_end_date;

																																						END IF;

																																		-- ELSIF v_tb_rec(i).duty_override = 'Y'
																																		-- THEN

																																						-- IF v_tb_rec(i).preferential_rate = 'Y'
																																						-- THEN
																																										-- v_tb_rec(i).duty_start_date := v_tb_rec(i).pr_start_date;
																																										-- v_tb_rec(i).duty_end_date := v_tb_rec(i).pr_end_date;

																																										-- write_log_prc(v_tb_rec(i).duty);

																																						-- ELSIF v_tb_rec(i).preferential_rate = 'N'
																																						-- THEN
																																										-- v_tb_rec(i).duty_start_date := v_tb_rec(i).effective_start_date;
																																										-- v_tb_rec(i).duty_end_date := v_tb_rec(i).effective_end_date;

																																									 -- write_log_prc(v_tb_rec(i).duty);

																																						-- END IF;

																																		END IF; -- Duty Rate Not Null

																												END LOOP;

																									EXCEPTION
																														WHEN OTHERS
																														THEN
																																			NULL;
																																			write_log_prc (SQLERRM||' Other Error - Duty Validations Failed');
																									END;

																									write_log_prc ('Duty Values Updated Process Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

																									BEGIN

																														FORALL i IN v_tb_rec.first .. v_tb_rec.last SAVE EXCEPTIONS

																																				UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t
																																							SET duty = v_tb_rec(i).duty
																																										,duty_start_date = v_tb_rec(i).duty_start_date
																																										,duty_end_date = v_tb_rec(i).duty_end_date
																																										,rec_status = v_tb_rec(i).rec_status
																																										,error_msg = v_tb_rec(i).error_msg
																																					WHERE rec_status = 'N'
																																							AND error_msg IS NULL
																																							AND active_flag = 'Y'
																																							AND request_id = gn_request_id
																																							AND inventory_item_id = v_tb_rec(i).inventory_item_id
																																							AND inventory_org_id = v_tb_rec(i).inventory_org_id
																																							--AND country_of_origin = v_tb_rec(i).country_of_origin
																																							AND rowid = v_tb_rec(i).rowid;

                                    write_log_prc (SQL%ROWCOUNT||' Duty Values Updated...');
																																				COMMIT;

																									EXCEPTION
																														WHEN e_bulk_errors
																														THEN
																																	write_log_prc ('Inside E_BULK_ERRORS');
																																	l_error_count := SQL%BULK_EXCEPTIONS.COUNT;

																																	FOR i IN 1 .. l_error_count
																																	LOOP
																																				l_msg := SQLERRM (-SQL%BULK_EXCEPTIONS (i).ERROR_CODE);
																																				l_idx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
																																				write_log_prc ('Failed to update Dute Values Element For Inventory Item- '
																																																			|| v_tb_rec (l_idx).inventory_item_id
																																																			|| 'Org Id-'
																																																			|| v_tb_rec (l_idx).inventory_org_id
																																																			|| 'COO-'
																																																			|| v_tb_rec (l_idx).country_of_origin
																																																			|| ' with error_code- '
																																																			|| l_msg);
																																	END LOOP;

																														WHEN OTHERS 
																														THEN
																																		write_log_prc ('Update Failed for Error Records' || SQLERRM);
																									END;

																									write_log_prc ('Duty Values Updated Process Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

														       END IF;

												     EXIT WHEN c_subelements%NOTFOUND;																	
												     END LOOP;

												     CLOSE c_subelements;

																	COMMIT;
																	write_log_prc ('Duty Calculation Process Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

												-- IF (pv_region IS NULL AND pv_inv_org IS NULL)
												-- THEN
																	-- apps.fnd_file.put_line(apps.fnd_file.log,' Please Process the Program with Region and Inventory ORG Parameter...');
																	-- retcode := gn_warning;

												-- ELSE

																SELECT COUNT(1)
																		INTO ln_ele_rec_success
																		FROM xxdo.xxd_cst_duty_ele_upld_stg_t
																	WHERE request_id = gn_request_id
																			AND rec_status = 'N'
																			AND active_flag = 'Y';

																SELECT COUNT(1)
																		INTO ln_ele_rec_error
																		FROM xxdo.xxd_cst_duty_ele_upld_stg_t
																	WHERE request_id = gn_request_id
																			AND rec_status = 'E'
																			AND active_flag = 'Y';

																apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Successfully Validated into Process Staging Table            - '||ln_ele_rec_success);
																apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Errored in Process Staging Table Post Validation             - '||ln_ele_rec_error);
																apps.fnd_file.put_line(apps.fnd_file.output,'*************************************************************************************************');

																IF pv_mode = 'Preview'
																THEN

																				write_duty_ele_report_prc (pv_region, 
																																															pv_inv_org, 
																																															pv_style, 
																																															pv_color, 
																																															pv_mode, 
																																															pv_dis_sku,
																																															pv_send_mail,
																																															lv_rep_file_name);

																				IF NVL (pv_send_mail,'N') = 'Y'
																				THEN
																								duty_ele_rep_send_mail_prc (lv_rep_file_name
																																																			,lv_err_code);

																								retcode := lv_err_code;

																				END IF;

																ELSIF pv_mode = 'Process'
																THEN	

																				write_log_prc ('Procedure insert_into_interface_prc Process Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

																				write_log_prc ('Truncating table CST_ITEM_CST_DTLS_INTERFACE.. ');

																				EXECUTE IMMEDIATE 'truncate table BOM.CST_ITEM_CST_DTLS_INTERFACE';

																				BEGIN
																							SELECT cc.cost_element_id
																									INTO gn_cost_element_id
																									FROM cst_cost_elements cc
																								WHERE 1 = 1
																										AND UPPER (cc.cost_element) = 'MATERIAL OVERHEAD';

																							write_log_prc ('cost element id for material_overhead is -'|| gn_cost_element_id);

																				EXCEPTION
																									WHEN OTHERS
																									THEN
																													write_log_prc ( 'Error while fetching cost element id ');
																													RAISE user_exception;
																				END;

																				FOR i IN c_subelements
																				LOOP
																								l_group_id := i.group_id;
																								l_insert_count := l_insert_count + 1;
																								l_price := 0;

																								IF i.coo_preference_flag = 'Y'
																								THEN

																												BEGIN
																																	SELECT default_basis_type
																																			INTO l_duty_basis
																																			FROM bom_resources_v
																																		WHERE 1 = 1
																																				AND cost_element_id = gn_cost_element_id
																																				AND resource_code = gc_duty
																																				AND organization_id = i.inventory_org_id;

																																	-- write_log_prc ('Deriving default_basis_type for Duty - '||l_duty_basis);

																												EXCEPTION
																																	WHEN OTHERS
																																	THEN
																																						l_duty_basis := 1;
																												END;

																												BEGIN
																																	SELECT default_basis_type
																																			INTO l_oh_duty_basis
																																			FROM bom_resources_v
																																		WHERE 1 = 1
																																				AND cost_element_id = gn_cost_element_id
																																				AND resource_code = gc_oh_duty
																																				AND organization_id = i.inventory_org_id;

																															-- write_log_prc ('Deriving default_basis_type for OH Duty - '||l_oh_duty_basis);

																												EXCEPTION
																																	WHEN OTHERS
																																	THEN
																																					l_oh_duty_basis := 1;
																												END;

																											BEGIN
																																SELECT default_basis_type
																																		INTO l_oh_nonduty_basis
																																		FROM bom_resources_v
																																	WHERE 1 = 1
																																			AND cost_element_id = gn_cost_element_id
																																			AND resource_code = gc_oh_nonduty
																																			AND organization_id = i.inventory_org_id;

																															-- write_log_prc ('Deriving default_basis_type for OH Non Duty - '||l_oh_nonduty_basis);

																											EXCEPTION
																																WHEN OTHERS
																																THEN
																																				l_oh_nonduty_basis := 1;
																											END;

																												BEGIN
																																	SELECT default_basis_type
																																			INTO l_freight_basis
																																			FROM bom_resources_v
																																		WHERE 1 = 1
																																				AND cost_element_id = gn_cost_element_id
																																				AND resource_code = gc_freight
																																				AND organization_id = i.inventory_org_id;

																															-- write_log_prc ('Deriving default_basis_type for Freight - '||l_freight_basis);

																												EXCEPTION
																																	WHEN OTHERS
																																	THEN
																																					l_freight_basis := 1;
																												END;

																												BEGIN
																																	SELECT default_basis_type
																																			INTO l_freight_du_basis
																																			FROM bom_resources_v
																																		WHERE 1 = 1
																																				AND cost_element_id = gn_cost_element_id
																																				AND resource_code = gc_freight_du
																																				AND organization_id = i.inventory_org_id;

																															-- write_log_prc ('Deriving default_basis_type for Freight Duty - '||l_freight_du_basis);

																												EXCEPTION
																																	WHEN OTHERS
																																	THEN
																																					l_freight_du_basis := 1;
																												END;

																												IF i.duty IS NULL
																												THEN
																																BEGIN

																																					lv_duty := NULL;

																																					SELECT usage_rate_or_amount
																																							INTO lv_duty
																																							FROM apps.cst_item_cost_details_v
																																						WHERE resource_code = gc_duty
																																								AND cost_type_id = 1000
																																								AND inventory_item_id = i.inventory_item_id
																																								AND organization_id = i.inventory_org_id;

																																EXCEPTION
																																					WHEN OTHERS
																																					THEN
																																									write_log_prc ('Unable to Derive Duty Element from CICD');
																																									lv_duty := NULL;
																																END;

																												ELSIF i.duty IS NOT NULL
																												THEN
																												    lv_duty := NULL;
																																lv_duty := i.duty;
																												END IF;

																												IF lv_duty IS NOT NULL
																												THEN
																												-- IF i.duty IS NOT NULL
																												-- THEN
																																-- write_log_prc ('Inserting Duty Element Data into Interface');
																																write_log_prc (i.inventory_item_id||'-'||
																																																																									i.inventory_org_id||'-'||
																																																																									gc_duty||'-'||
																																																																									i.duty||'-'||
																																																																									gn_cost_element_id||'-'||
																																																																									gc_cost_type||'-'||
																																																																									l_duty_basis||'-'||
																																																																									gn_process_flag||'-'||
																																																																									SYSDATE||'-'||
																																																																									gn_user_id||'-'||
																																																																									SYSDATE||'-'||
																																																																									gn_user_id||'-'||
																																																																									l_group_id||'-'||
																																																																									i.country_of_origin);

																																INSERT INTO cst_item_cst_dtls_interface (
																																																																									inventory_item_id,
																																																																									organization_id,
																																																																									resource_code,
																																																																									usage_rate_or_amount,
																																																																									cost_element_id,
																																																																									cost_type,
																																																																									basis_type,
																																																																									process_flag,
																																																																									last_update_date,
																																																																									last_updated_by,
																																																																									creation_date,
																																																																									created_by,
																																																																									group_id,
																																																																									attribute15
																																																																									)
																																																																		VALUES (
																																																																									i.inventory_item_id,
																																																																									i.inventory_org_id,
																																																																									gc_duty,
																																																																									-- i.duty
																																																																									lv_duty,
																																																																									gn_cost_element_id,
																																																																									gc_cost_type,
																																																																									l_duty_basis,
																																																																									gn_process_flag,
																																																																									SYSDATE,
																																																																									gn_user_id,
																																																																									SYSDATE,
																																																																									gn_user_id,
																																																																									l_group_id,
																																																																									i.country_of_origin
																																																																									);
																												END IF;

/*																												IF i.freight IS NULL OR i.oh_duty IS NULL OR i.oh_nonduty IS NULL OR i.freight_duty IS NULL
																												THEN
																																	IF i.freight IS NULL
																																	THEN
																																					BEGIN

																																										lv_freight := NULL;

																																										SELECT usage_rate_or_amount
																																												INTO lv_freight
																																												FROM apps.cst_item_cost_details_v
																																											WHERE resource_code = gc_freight
																																													AND cost_type_id = 1000
																																													AND inventory_item_id = i.inventory_item_id
																																													AND organization_id = i.inventory_org_id;

																																					EXCEPTION
																																										WHEN OTHERS
																																										THEN
																																														write_log_prc ('Unable to Derive Freight Element from CICD');
																																														lv_freight := NULL;
																																					END;

																																	ELSIF i.freight IS NOT NULL
																																	THEN
																																					lv_freight := NULL;
																																					lv_freight := i.freight;
																																	END IF;

																																	IF i.oh_duty IS NULL
																																	THEN
																																					BEGIN

																																										lv_oh_duty := NULL;

																																										SELECT usage_rate_or_amount
																																												INTO lv_oh_duty
																																												FROM apps.cst_item_cost_details_v
																																											WHERE resource_code = gc_oh_duty
																																													AND cost_type_id = 1000
																																													AND inventory_item_id = i.inventory_item_id
																																													AND organization_id = i.inventory_org_id;

																																					EXCEPTION
																																										WHEN OTHERS
																																										THEN
																																														write_log_prc ('Unable to Derive OH Duty Element from CICD');
																																														lv_oh_duty := NULL;
																																					END;

																																	ELSIF i.oh_duty IS NOT NULL
																																	THEN
																																					lv_oh_duty := NULL;
																																					lv_oh_duty := i.oh_duty;
																																	END IF;

																																	IF i.oh_nonduty IS NULL
																																	THEN
																																					BEGIN

																																										lv_oh_nonduty := NULL;

																																										SELECT usage_rate_or_amount
																																												INTO lv_oh_nonduty
																																												FROM apps.cst_item_cost_details_v
																																											WHERE resource_code = gc_oh_nonduty
																																													AND cost_type_id = 1000
																																													AND inventory_item_id = i.inventory_item_id
																																													AND organization_id = i.inventory_org_id;

																																					EXCEPTION
																																										WHEN OTHERS
																																										THEN
																																														write_log_prc ('Unable to Derive OH NONDuty Element from CICD');
																																														lv_oh_nonduty := NULL;
																																					END;

																																	ELSIF i.oh_nonduty IS NOT NULL
																																	THEN
																																					lv_oh_nonduty := NULL;
																																					lv_oh_nonduty := i.oh_nonduty;
																																	END IF;

																																	IF i.freight_duty IS NULL
																																	THEN
																																					BEGIN

																																										lv_freight_duty := NULL;

																																										SELECT usage_rate_or_amount
																																												INTO lv_freight_duty
																																												FROM apps.cst_item_cost_details_v
																																											WHERE resource_code = gc_freight_du
																																													AND cost_type_id = 1000
																																													AND inventory_item_id = i.inventory_item_id
																																													AND organization_id = i.inventory_org_id;

																																					EXCEPTION
																																										WHEN OTHERS
																																										THEN
																																														write_log_prc ('Unable to Derive OH NONDuty Element from CICD');
																																														lv_freight_duty := NULL;
																																					END;

																																	ELSIF i.freight_duty IS NOT NULL
																																	THEN
																																					lv_freight_duty := NULL;
																																					lv_freight_duty := i.freight_duty;
																																	END IF;

																																	IF lv_freight IS NULL OR lv_oh_duty IS NULL OR lv_oh_nonduty IS NULL OR lv_freight_duty IS NULL 
																																	THEN
																																	    lv_freight := 0;
																																					lv_oh_duty := 0;
																																					lv_oh_nonduty := 0;
																																					lv_freight_duty := 0;
																																	END IF;

																												ELSIF i.freight IS NOT NULL AND i.oh_duty IS NOT NULL AND i.oh_nonduty IS NOT NULL AND i.freight_duty IS NOT NULL
																												THEN
																																lv_freight := i.freight;
																																lv_oh_duty := i.oh_duty;
																																lv_oh_nonduty := i.oh_nonduty;
																																lv_freight_duty := i.freight_duty;																																	
																											 END IF;	*/

																												IF i.freight IS NULL
																												THEN
																																BEGIN

																																					lv_freight := 0;

																																					SELECT usage_rate_or_amount
																																							INTO lv_freight
																																							FROM apps.cst_item_cost_details_v
																																						WHERE resource_code = gc_freight
																																								AND cost_type_id = 1000
																																								AND inventory_item_id = i.inventory_item_id
																																								AND organization_id = i.inventory_org_id;

																																EXCEPTION
																																					WHEN OTHERS
																																					THEN
																																									write_log_prc ('Unable to Derive Freight Element from CICD');
																																									lv_freight := 0;
																																END;

																												ELSIF i.freight IS NOT NULL
																												THEN
																																lv_freight := 0;
																																lv_freight := i.freight;
																												END IF;

																												IF lv_freight IS NOT NULL
																												THEN

																																-- write_log_prc ('Inserting freight Element Data into Interface');
																																INSERT INTO cst_item_cst_dtls_interface (
																																																																									inventory_item_id,
																																																																									organization_id,
																																																																									resource_code,
																																																																									usage_rate_or_amount,
																																																																									cost_element_id,
																																																																									cost_type,
																																																																									basis_type,
																																																																									process_flag,
																																																																									last_update_date,
																																																																									last_updated_by,
																																																																									creation_date,
																																																																									created_by,
																																																																									group_id,
																																																																									attribute15
																																																																									)
																																																																		VALUES (
																																																																									i.inventory_item_id,
																																																																									i.inventory_org_id,
																																																																									gc_freight,
																																																																									-- i.freight,
																																																																									lv_freight,
																																																																									gn_cost_element_id,
																																																																									gc_cost_type,
																																																																									l_freight_basis,
																																																																									gn_process_flag,
																																																																									SYSDATE,
																																																																									gn_user_id,
																																																																									SYSDATE,
																																																																									gn_user_id,
																																																																									l_group_id,
																																																																									i.country_of_origin
																																																																									);
																												END IF;

																												IF i.oh_duty IS NULL
																												THEN
																																BEGIN

																																					lv_oh_duty := 0;

																																					SELECT usage_rate_or_amount
																																							INTO lv_oh_duty
																																							FROM apps.cst_item_cost_details_v
																																						WHERE resource_code = gc_oh_duty
																																								AND cost_type_id = 1000
																																								AND inventory_item_id = i.inventory_item_id
																																								AND organization_id = i.inventory_org_id;

																																EXCEPTION
																																					WHEN OTHERS
																																					THEN
																																									write_log_prc ('Unable to Derive OH Duty Element from CICD');
																																									lv_oh_duty := 0;
																																END;

																												ELSIF i.oh_duty IS NOT NULL
																												THEN
																																lv_oh_duty := 0;
																																lv_oh_duty := i.oh_duty;
																												END IF;

																												IF lv_oh_duty IS NOT NULL
																												THEN
																																-- write_log_prc ('Inserting OH Duty Element Data into Interface');
																																INSERT INTO cst_item_cst_dtls_interface (
																																																																									inventory_item_id,
																																																																									organization_id,
																																																																									resource_code,
																																																																									usage_rate_or_amount,
																																																																									cost_element_id,
																																																																									cost_type,
																																																																									basis_type,
																																																																									process_flag,
																																																																									last_update_date,
																																																																									last_updated_by,
																																																																									creation_date,
																																																																									created_by,
																																																																									group_id,
																																																																									attribute15
																																																																									)
																																																																		VALUES (
																																																																									i.inventory_item_id,
																																																																									i.inventory_org_id,
																																																																									gc_oh_duty,
																																																																									--i.oh_duty,
																																																																									lv_oh_duty,
																																																																									gn_cost_element_id,
																																																																									gc_cost_type,
																																																																									l_oh_duty_basis,
																																																																									gn_process_flag,
																																																																									SYSDATE,
																																																																									gn_user_id,
																																																																									SYSDATE,
																																																																									gn_user_id,
																																																																									l_group_id,
																																																																									i.country_of_origin
																																																																									);
																												END IF;

																												IF i.oh_nonduty IS NULL
																												THEN
																																BEGIN

																																					lv_oh_nonduty := 0;

																																					SELECT usage_rate_or_amount
																																							INTO lv_oh_nonduty
																																							FROM apps.cst_item_cost_details_v
																																						WHERE resource_code = gc_oh_nonduty
																																								AND cost_type_id = 1000
																																								AND inventory_item_id = i.inventory_item_id
																																								AND organization_id = i.inventory_org_id;

																																EXCEPTION
																																					WHEN OTHERS
																																					THEN
																																									write_log_prc ('Unable to Derive OH NONDuty Element from CICD');
																																									lv_oh_nonduty := 0;
																																END;

																												ELSIF i.oh_nonduty IS NOT NULL
																												THEN
																																lv_oh_nonduty := 0;
																																lv_oh_nonduty := i.oh_nonduty;
																												END IF;

																												IF lv_oh_nonduty IS NOT NULL
																												THEN
																																-- write_log_prc ('Inserting OH Non Duty Element Data into Interface');
																																INSERT INTO cst_item_cst_dtls_interface (
																																																																									inventory_item_id,
																																																																									organization_id,
																																																																									resource_code,
																																																																									usage_rate_or_amount,
																																																																									cost_element_id,
																																																																									cost_type,
																																																																									basis_type,
																																																																									process_flag,
																																																																									last_update_date,
																																																																									last_updated_by,
																																																																									creation_date,
																																																																									created_by,
																																																																									group_id,
																																																																									attribute15
																																																																									)
																																																																			VALUES 
																																																																									(
																																																																										i.inventory_item_id,
																																																																										i.inventory_org_id,
																																																																										gc_oh_nonduty,
																																																																										--i.oh_nonduty,
																																																																										lv_oh_nonduty,
																																																																										gn_cost_element_id,
																																																																										gc_cost_type,
																																																																										l_oh_nonduty_basis,
																																																																										gn_process_flag,
																																																																										SYSDATE,
																																																																										gn_user_id,
																																																																										SYSDATE,
																																																																										gn_user_id,
																																																																										l_group_id,
																																																																										i.country_of_origin
																																																																										);
																												END IF;

																												IF i.freight_duty IS NULL
																												THEN
																																BEGIN

																																					lv_freight_duty := 0;

																																					SELECT usage_rate_or_amount
																																							INTO lv_freight_duty
																																							FROM apps.cst_item_cost_details_v
																																						WHERE resource_code = gc_freight_du
																																								AND cost_type_id = 1000
																																								AND inventory_item_id = i.inventory_item_id
																																								AND organization_id = i.inventory_org_id;

																																EXCEPTION
																																					WHEN OTHERS
																																					THEN
																																									write_log_prc ('Unable to Derive OH NONDuty Element from CICD');
																																									lv_freight_duty := 0;
																																END;

																												ELSIF i.freight_duty IS NOT NULL
																												THEN
																																lv_freight_duty := 0;
																																lv_freight_duty := i.freight_duty;
																												END IF;

																												IF lv_freight_duty IS NOT NULL
																												THEN
																																-- write_log_prc ('Inserting freight_duty Element Data into Interface');
																																INSERT INTO cst_item_cst_dtls_interface (
																																																																									inventory_item_id,
																																																																									organization_id,
																																																																									resource_code,
																																																																									usage_rate_or_amount,
																																																																									cost_element_id,
																																																																									cost_type,
																																																																									basis_type,
																																																																									process_flag,
																																																																									last_update_date,
																																																																									last_updated_by,
																																																																									creation_date,
																																																																									created_by,
																																																																									group_id,
																																																																									attribute15
																																																																									)
																																																																			VALUES 
																																																																									(
																																																																									i.inventory_item_id,
																																																																									i.inventory_org_id,
																																																																									gc_freight_du,
																																																																									-- i.freight_duty,
																																																																									lv_freight_duty,
																																																																									gn_cost_element_id,
																																																																									gc_cost_type,
																																																																									l_freight_du_basis,
																																																																									gn_process_flag,
																																																																									SYSDATE,
																																																																									gn_user_id,
																																																																									SYSDATE,
																																																																									gn_user_id,
																																																																									l_group_id,
																																																																									i.country_of_origin
																																																																									);
																												END IF;

																												BEGIN
																																	SELECT list_price_per_unit
																																			INTO l_price
																																			FROM mtl_system_items_b msi
																																		WHERE 1 = 1
																																				AND msi.inventory_item_id = i.inventory_item_id
																																				AND organization_id = i.inventory_org_id;

																																	write_log_prc ('List Price Per Unit :'||l_price);

																												EXCEPTION
																																	WHEN OTHERS
																																	THEN
																																					l_price := 1;
																																					write_log_prc ('Error while fetching list price of the item ');
																												END;

																												IF (l_price = 0 OR l_price IS NULL) AND i.factory_cost IS NOT NULL
																												THEN
																																write_log_prc ('updating list_price_per_unit for item - '|| i.inventory_item_id	|| ' in org - '	|| i.inventory_org_id);
																																api_index := api_index + 1;

																															l_item_tbl_typ (api_index).transaction_type := 'UPDATE';
																															l_item_tbl_typ (api_index).inventory_item_id := i.inventory_item_id;
																															l_item_tbl_typ (api_index).organization_id := i.inventory_org_id;
																															l_item_tbl_typ (api_index).list_price_per_unit := i.factory_cost;

																												END IF;

																												IF l_insert_count >= 2000
																												THEN
																															COMMIT;
																															l_insert_count := 0;

																															update_item_price_prc (l_item_tbl_typ);
																															api_index := 0;
																															l_item_tbl_typ.delete;
																												END IF;

																								END IF;				

																				EXIT WHEN c_subelements%NOTFOUND;				
																				END LOOP;

																				update_item_price_prc (l_item_tbl_typ);
																				l_item_tbl_typ.delete;

																				COMMIT;

																				BEGIN
																									UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t xcdeus
																												SET xcdeus.rec_status = 'I'
																										WHERE EXISTS
																																						(SELECT 1
																																									FROM cst_item_cst_dtls_interface
																																								WHERE 1 = 1
																																										AND TO_NUMBER (xcdeus.inventory_item_id) = inventory_item_id
																																										AND TO_NUMBER (xcdeus.inventory_org_id) = organization_id
																																										-- AND xcdeus.country_of_origin = attribute15
																																									 -- AND group_id = l_group_id
																																										AND error_flag IS NULL
																																										AND process_flag = 1)
																												AND xcdeus.rec_status = 'N'
																												AND xcdeus.error_msg IS NULL
																												AND xcdeus.active_flag = 'Y'
																											 -- AND xcdeus.group_id = l_group_id
																												AND xcdeus.request_id = gn_request_id
																												AND xcdeus.coo_preference_flag = 'Y';

																				     write_log_prc (SQL%ROWCOUNT||' Records successfully Updated with status I for coo_preference_flag Y Records');

																									COMMIT;

																				EXCEPTION
																									WHEN OTHERS
																									THEN
																													write_log_prc ('Exception Occured while updating status I to staging table:');
																				END;

																				BEGIN
																									UPDATE xxdo.xxd_cst_duty_ele_upld_stg_t xcdeus
																												SET xcdeus.rec_status = 'C'
																										WHERE 1 = 1
																										      -- NOT EXISTS
																																										-- (SELECT 1
																																													-- FROM cst_item_cst_dtls_interface
																																												-- WHERE 1 = 1
																																														-- AND xcdeus.inventory_item_id = inventory_item_id
																																														-- AND xcdeus.inventory_org_id = organization_id
																																														-- AND xcdeus.country_of_origin = attribute15
																																														-- AND error_flag IS NULL
																																														-- AND process_flag = 1)
																												AND xcdeus.rec_status = 'N'
																												AND xcdeus.error_msg IS NULL
																												AND xcdeus.active_flag = 'Y'
																												AND xcdeus.request_id = gn_request_id
																												AND xcdeus.coo_preference_flag = 'N';

																				     write_log_prc (SQL%ROWCOUNT||' Records successfully Updated with status NA for coo_preference_flag N Records ');

																									COMMIT;

																				EXCEPTION
																									WHEN OTHERS
																									THEN
																													write_log_prc ('Exception Occured while updating status NA to staging table:');
																				END;

																				BEGIN
																							SELECT DISTINCT 'Y'
																									INTO v_interfaced
																									FROM xxdo.xxd_cst_duty_ele_upld_stg_t
																								WHERE EXISTS (SELECT DISTINCT rec_status
																																								FROM xxdo.xxd_cst_duty_ele_upld_stg_t
																																							WHERE 1 = 1
																																									AND rec_status = 'I'
																																									AND active_flag = 'Y'
																																									AND request_id = gn_request_id
																																									AND coo_preference_flag = 'Y');

																				EXCEPTION
																									WHEN OTHERS
																									THEN
																													write_log_prc ('Exception Occured while fetching v_interfaced:'||SQLERRM);
																													v_interfaced := 'N';
																				END;

																				write_log_prc ('v_interfaced = ' || v_interfaced);
																				write_log_prc ('group_id = ' || l_group_id);

																				IF v_interfaced = 'Y'
																				THEN

																				   FOR I IN c_group_id
																							LOOP
																								   write_log_prc ('submitting Cost Import program for the Group ID: '||i.group_id);

																											submit_cost_import_prc (p_errbuff,
																																																			p_retcode,
																																																			l_int_req_id,
																																																			i.group_id);

																											write_log_prc ('updating interfcae status into staging table for the Group ID: '||i.group_id);

																											update_interface_status_prc (l_int_req_id,
																																																								i.group_id);


                           --l_int_req_id := l_int_req_id||',';

																							EXIT WHEN c_group_id%NOTFOUND;
																							END LOOP;

																								-- write_log_prc ('submitting custom table update program');

																								BEGIN
																													SELECT DISTINCT 'Y'
																															INTO v_processed
																															FROM xxdo.xxd_cst_duty_ele_upld_stg_t
																														WHERE EXISTS
																																									(SELECT DISTINCT rec_status
																																												FROM xxdo.xxd_cst_duty_ele_upld_stg_t
																																											WHERE 1 = 1
																																													AND rec_status = 'P'
																																													AND active_flag = 'Y'
																																													AND request_id = gn_request_id);
																								EXCEPTION
																													WHEN OTHERS
																													THEN
																																	write_log_prc ('Exception Occured while fetching v_processed:');
																																	v_processed := 'N';
																								END;

																								IF v_processed = 'Y'
																								THEN

																												-- insert_into_invval_duty_cost_prc;  -- Commented for CCR0010051

																												write_duty_ele_report_prc (pv_region, 
																																																							pv_inv_org, 
																																																							pv_style, 
																																																							pv_color,
																																																							pv_mode,
																																																							pv_dis_sku,
																																																							pv_send_mail,
																																																							lv_rep_file_name);

																												IF NVL (pv_send_mail,'N') = 'Y'
																												THEN
																																duty_ele_rep_send_mail_prc (lv_rep_file_name
																																																											,lv_err_code);	

																																retcode := lv_err_code;
																												END IF;

																												write_log_prc ('submitting custom table upload program');

																												insert_into_invval_duty_cost_prc;   -- Added for CCR0010051

																								END IF; -- v_processed

																				END IF; -- v_interfaced

																END IF; -- pv_mode

									       write_log_prc ('Procedure insert_into_interface_prc Process Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

												-- END IF; -- pv_region

												IF pv_mode = 'Process'
												THEN	

																BEGIN

																					SELECT COUNT (1)
																							INTO ln_rec_success
																							FROM xxdo.xxd_cst_duty_ele_upld_stg_t xcdeus
																						WHERE xcdeus.rec_status = 'P'
																								AND xcdeus.error_msg IS NULL
																								AND xcdeus.active_flag = 'Y'
																								AND xcdeus.request_id = gn_request_id;

																					SELECT COUNT (1)
																							INTO ln_rec_fail
																							FROM xxdo.xxd_cst_duty_ele_upld_stg_t xcdeus
																						WHERE xcdeus.rec_status = 'E'
																								AND xcdeus.error_msg IS NOT NULL
																								AND xcdeus.active_flag = 'Y'
																								AND xcdeus.request_id = gn_request_id;

																				 SELECT COUNT(1)
																					  INTO ln_rec_oracle
																					  FROM cst_item_cst_dtls_interface
																						--WHERE request_id = l_int_req_id;
																						WHERE group_id in (SELECT DISTINCT group_id
																						                     FROM xxdo.xxd_cst_duty_ele_upld_stg_t
																																										WHERE request_id = gn_request_id);

																				 SELECT COUNT(1)
																					  INTO ln_rec_success_oracle
																					  FROM cst_item_cst_dtls_interface
																						--WHERE request_id = l_int_req_id
																						WHERE group_id in (SELECT DISTINCT group_id
																						                     FROM xxdo.xxd_cst_duty_ele_upld_stg_t
																																										WHERE request_id = gn_request_id)
																						  AND process_flag = 5;

																						ln_rec_fail_oracle := ln_rec_oracle - ln_rec_success_oracle;

																					SELECT COUNT (1)
																							INTO ln_rec_na
																							FROM xxdo.xxd_cst_duty_ele_upld_stg_t xcdeus
																						WHERE xcdeus.rec_status = 'C'
																								AND xcdeus.error_msg IS NULL
																								AND xcdeus.active_flag = 'Y'
																								AND xcdeus.request_id = gn_request_id;

																EXCEPTION
																					WHEN OTHERS
																					THEN
																										write_log_prc ('Exception Occured while retriving the records status count');
																END;

																apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Inserted into Interface                                      - '||ln_rec_oracle);
																apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Successful in Interface table                                - '||ln_rec_success_oracle);
																apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Errored in Interface table                                   - '||ln_rec_fail_oracle);
                apps.fnd_file.put_line(apps.fnd_file.output,'*************************************************************************************************');
																apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Successful in Process Staging table After Cost Import        - '||ln_rec_success);
																apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Not Considered in Process Staging table by coo preference    - '||ln_rec_na);
																apps.fnd_file.put_line(apps.fnd_file.output,' Total Errored Records in Process Staging Table After Cost Import            - '||ln_rec_fail);																
																apps.fnd_file.put_line(apps.fnd_file.output,'*************************************************************************************************');
																apps.fnd_file.put_line(apps.fnd_file.output,'');
																apps.fnd_file.put_line(apps.fnd_file.output,'');
																apps.fnd_file.put_line(apps.fnd_file.output,'');
																apps.fnd_file.put_line(apps.fnd_file.output,'Distinct Error Messages :');
																apps.fnd_file.put_line(apps.fnd_file.output,'=========================');
																apps.fnd_file.put_line(apps.fnd_file.output,'Count'||CHR(9)||'Error Message');
																apps.fnd_file.put_line(apps.fnd_file.output,'-----------------------------------------------------------------');

																FOR i in c_write_errors
																LOOP
																				apps.fnd_file.put_line(apps.fnd_file.output, i.err_cnt||CHR(9)||i.error_msg);
																END LOOP;

												END IF;

												apps.fnd_file.put_line(apps.fnd_file.output,'');
												apps.fnd_file.put_line(apps.fnd_file.output,'');
												apps.fnd_file.put_line(apps.fnd_file.output,'');
												apps.fnd_file.put_line(apps.fnd_file.output,'**********************************************************************************************');
												apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Successfully Inserted into table xxdo_invval_duty_cost       - '||g_invval_cnt);
												apps.fnd_file.put_line(apps.fnd_file.output,'**********************************************************************************************');

								ELSIF ln_cnt = 0
								THEN
							    write_log_prc ('No Valid records are present in the xxdo.xxd_cst_duty_ele_upld_stg_t table and SQLERRM' || SQLERRM);
											apps.fnd_file.put_line(apps.fnd_file.output,'No Valid records are present in the xxdo.xxd_cst_duty_ele_upld_stg_t table');
							 END IF;

							 write_log_prc ('Procedure insert_into_interface_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

	  EXCEPTION
								WHEN user_exception
								THEN
												write_log_prc ('Cost element not found');
												write_log_prc ('Error in Procedure insert_into_interface_prc:' || SQLERRM);
												retcode := 2;
								WHEN OTHERS
								THEN
												retcode := 2;
												write_log_prc ('Error in Procedure insert_into_interface_prc:' || SQLERRM);
	END insert_into_interface_prc;

	/***************************************************************************
	-- PROCEDURE get_category_id_prc
	-- PURPOSE: This Procedure get the category_id based on Category Record
	***************************************************************************/
	PROCEDURE get_category_id_prc (p_processing_row_id   IN     NUMBER,
																												    x_return_status         OUT VARCHAR2)
	AS
				l_category_rec      inv_item_category_pub.category_rec_type;
				l_category_set_id   mtl_category_sets_v.category_set_id%TYPE;
				l_segment_array     fnd_flex_ext.segmentarray;
				l_n_segments        NUMBER := 0;
				l_delim             VARCHAR2 (1000);
				l_success           BOOLEAN;
				l_concat_segs       VARCHAR2 (32000);
				l_concat_segments   VARCHAR2 (32000);
				l_return_status     VARCHAR2 (80);
				l_error_code        NUMBER;
				l_msg_count         NUMBER;
				ln_category_id      NUMBER;
				l_msg_data          VARCHAR2 (32000);
				l_messages          VARCHAR2 (32000) := '';
				l_out_category_id   NUMBER;
				x_message_list      error_handler.error_tbl_type;
				x_msg_data          VARCHAR2 (32000);
				l_seg_description   VARCHAR2 (32000); ----new segment added for concatenated segments

				CURSOR get_segments (l_structure_id NUMBER)
				IS
						SELECT application_column_name, ROWNUM
								FROM fnd_id_flex_segments
							WHERE 1 = 1
							  AND application_id = 401
									AND id_flex_code = 'MCAT'
									AND id_flex_num = l_structure_id
									AND enabled_flag = 'Y'
				ORDER BY segment_num ASC;

						CURSOR get_structure_id (cp_category_set_name VARCHAR2)
						IS
								SELECT structure_id, category_set_id
										FROM mtl_category_sets_v
									WHERE 1 = 1
									  AND category_set_name = cp_category_set_name;

					CURSOR get_category_id_prc (cp_structure_id NUMBER,
																													cp_concatenated_segs VARCHAR2)
					IS
							SELECT category_id
									FROM mtl_categories_b_kfv
								WHERE 1 = 1
								  AND structure_id = cp_structure_id
										AND concatenated_segments = cp_concatenated_segs;
	BEGIN
						write_log_prc ('Procedure get_category_id_prc Begins....'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
						l_return_status := fnd_api.g_ret_sts_success;

						FOR lc_cat_data IN (SELECT *
																											FROM xxdo.xxd_cst_duty_ele_cat_stg_t
																										WHERE record_id = p_processing_row_id)
						LOOP
										gn_category_id          := NULL;
										l_category_rec.segment1 := NULL;
										l_category_rec.segment2 := NULL;
										l_category_rec.segment3 := NULL;
										l_category_rec.segment4 := NULL;
										l_category_rec.segment5 := NULL;
										l_category_rec.segment6 := NULL;
										l_category_rec.segment7 := NULL;
										l_category_rec.segment8 := NULL;
										l_category_rec.segment9 := NULL;
										l_category_rec.segment10 := NULL;
										l_category_rec.segment11 := NULL;
										l_category_rec.segment12 := NULL;
										l_category_rec.segment13 := NULL;
										l_category_rec.segment14 := NULL;
										l_category_rec.segment15 := NULL;
										l_category_rec.segment16 := NULL;
										l_category_rec.segment17 := NULL;
										l_category_rec.segment18 := NULL;
										l_category_rec.segment19 := NULL;
										l_category_rec.segment20 := NULL;

										OPEN get_structure_id (cp_category_set_name => lc_cat_data.category_set_name);

										FETCH get_structure_id
										INTO l_category_rec.structure_id, l_category_set_id;

										CLOSE get_structure_id;


										gn_category_set_id := l_category_set_id;
										--   SELECT f.id_flex_num
										--     INTO l_category_rec.structure_id
										--     FROM fnd_id_flex_structures f
										--    WHERE f.id_flex_structure_code = 'TOPPS ITEM CAT';


										-- Looping through the enabled segments in the target instance
										-- and setting the values for only those segments those are enabled
										l_seg_description := NULL;

										-- gn_category_id := NULL;

										FOR c_segments IN get_segments (l_category_rec.structure_id)
										LOOP
														l_n_segments := c_segments.ROWNUM;

													IF c_segments.application_column_name = 'SEGMENT1'
													THEN
																l_category_rec.segment1 := lc_cat_data.segment1;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment1;
													ELSIF c_segments.application_column_name = 'SEGMENT2'
													THEN
																l_category_rec.segment2 := lc_cat_data.segment2;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment2;
													--           IF lc_cat_data.SEGMENT2 IS NULL AND lc_cat_data.SEGMENT3 IS NOT NULL THEN
													--               l_segment_array(c_segments.rownum):= '.';
													--            END IF;

													ELSIF c_segments.application_column_name = 'SEGMENT3'
													THEN
																l_category_rec.segment3 := lc_cat_data.segment3;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment3;
													ELSIF c_segments.application_column_name = 'SEGMENT4'
													THEN
																l_category_rec.segment4 := lc_cat_data.segment4;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment4;
													ELSIF c_segments.application_column_name = 'SEGMENT5'
													THEN
																l_category_rec.segment5 := lc_cat_data.segment5;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment5;
													ELSIF c_segments.application_column_name = 'SEGMENT6'
													THEN
																l_category_rec.segment6 := lc_cat_data.segment6;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment6;
													ELSIF c_segments.application_column_name = 'SEGMENT7'
													THEN
																l_category_rec.segment7 := lc_cat_data.segment7;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment7;
													ELSIF c_segments.application_column_name = 'SEGMENT8'
													THEN
																l_category_rec.segment8 := lc_cat_data.segment8;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment8;
													ELSIF c_segments.application_column_name = 'SEGMENT9'
													THEN
																l_category_rec.segment9 := lc_cat_data.segment9;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment9;
													ELSIF c_segments.application_column_name = 'SEGMENT10'
													THEN
																l_category_rec.segment10 := lc_cat_data.segment10;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment10;
													ELSIF c_segments.application_column_name = 'SEGMENT11'
													THEN
																l_category_rec.segment11 := lc_cat_data.segment11;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment11;
													ELSIF c_segments.application_column_name = 'SEGMENT12'
													THEN
																l_category_rec.segment12 := lc_cat_data.segment12;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment12;
													ELSIF c_segments.application_column_name = 'SEGMENT13'
													THEN
																l_category_rec.segment13 := lc_cat_data.segment13;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment13;
													ELSIF c_segments.application_column_name = 'SEGMENT14'
													THEN
																l_category_rec.segment14 := lc_cat_data.segment14;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment14;
													ELSIF c_segments.application_column_name = 'SEGMENT15'
													THEN
																l_category_rec.segment15 := lc_cat_data.segment15;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment15;
													ELSIF c_segments.application_column_name = 'SEGMENT16'
													THEN
																l_category_rec.segment16 := lc_cat_data.segment16;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment16;
													ELSIF c_segments.application_column_name = 'SEGMENT17'
													THEN
																l_category_rec.segment17 := lc_cat_data.segment17;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment17;
													ELSIF c_segments.application_column_name = 'SEGMENT18'
													THEN
																l_category_rec.segment18 := lc_cat_data.segment18;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment18;
													ELSIF c_segments.application_column_name = 'SEGMENT19'
													THEN
																l_category_rec.segment19 := lc_cat_data.segment19;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment19;
													ELSIF c_segments.application_column_name = 'SEGMENT20'
													THEN
																l_category_rec.segment20 := lc_cat_data.segment20;
																l_segment_array (c_segments.ROWNUM) := lc_cat_data.segment20;
													END IF;
													write_log_prc ('l_segment_array-'||l_segment_array (c_segments.ROWNUM));
										END LOOP; -- loop to get all the enabled segments in the target inst.


										l_delim := fnd_flex_ext.get_delimiter ('INV',
																																																	'MCAT',
																																																	l_category_rec.structure_id);

          write_log_prc ('l_n_segments:'||l_n_segments||' l_delim:'||l_delim);

										l_concat_segs := fnd_flex_ext.concatenate_segments (l_n_segments,
																																																														l_segment_array,
																																																														l_delim);

										write_log_prc  ('OPEN get_category_id_prc l_concat_segs  => ' || l_concat_segs);

										l_success := fnd_flex_keyval.validate_segs (
																																																						operation          => 'FIND_COMBINATION',
																																																						appl_short_name    => 'INV',
																																																						key_flex_code      => 'MCAT',
																																																						structure_number   => l_category_rec.structure_id,
																																																						concat_segments    => l_concat_segs
																																																						);
										write_log_prc  ('OPEN get_category_id_prc structure_id   => ' || l_category_rec.structure_id);
										write_log_prc  ('OPEN get_category_id_prc l_concat_segs  => ' || l_concat_segs);

										l_category_rec.description := l_concat_segs; -- commenting the l_concat_seg as we need to pass actual description

										/*print_msg_prc (
													p_debug     => gc_debug_flag,
													p_message   =>    'Lenght of l_concat_segs  => '
																												|| length(l_concat_segs));*/
										OPEN get_category_id_prc (l_category_rec.structure_id, l_concat_segs);

										FETCH get_category_id_prc INTO gn_category_id;

										CLOSE get_category_id_prc;

									write_log_prc  ('OPEN get_category_id_prc gn_category_id  => '|| gn_category_id);

									IF (NOT l_success) AND gn_category_id IS NULL
									THEN
														write_log_prc  ('OPEN get_category_id_prc l_success  => True');
														inv_item_category_pub.create_category (
																																																					p_api_version     => 1.0,
																																																					p_init_msg_list   => fnd_api.g_false,
																																																					p_commit          => fnd_api.g_false,
																																																					x_return_status   => l_return_status,
																																																					x_errorcode       => l_error_code,
																																																					x_msg_count       => l_msg_count,
																																																					x_msg_data        => l_msg_data,
																																																					p_category_rec    => l_category_rec,
																																																					x_category_id     => l_out_category_id
																																																					);

													IF (l_return_status = fnd_api.g_ret_sts_success)
													THEN
																	gn_category_id := l_out_category_id;
																	write_log_prc  ('Category Id: ' || gn_category_id);
													ELSE
																	gn_category_id := NULL;
													END IF;

													IF (l_return_status <> fnd_api.g_ret_sts_success)
													THEN
																	fnd_msg_pub.count_and_get (p_encoded   => 'F',
																																												p_count     => l_msg_count,
																																												p_data      => l_msg_data);
																	write_log_prc  ('Category Id: Inside1 ' || l_msg_data || l_error_code);

																	FOR k IN 1 .. l_msg_count
																	LOOP
																					l_messages :=	l_messages|| fnd_msg_pub.get (p_msg_index => k, p_encoded => 'F')|| ';';
																					write_log_prc  ('l_messages => ' || l_messages);
																	END LOOP;

																fnd_message.set_name ('FND', 'GENERIC-INTERNAL ERROR');
																fnd_message.set_token ('ROUTINE', 'Category Migration');
																fnd_message.set_token ('REASON', l_messages);
																--APP_EXCEPTION.RAISE_EXCEPTION;

																write_log_prc (fnd_message.get);

															xxd_common_utils.record_error (
																																														p_module       => 'INV',
																																														p_org_id       => gn_org_id,
																																														p_program      => 'Deckers Cost Elements Category Assignment Program',
																																														p_error_line   => SQLCODE,
																																														p_error_msg    => substr(l_messages,1,2000),
																																														p_created_by   => gn_user_id,
																																														p_request_id   => gn_conc_request_id,
																																														p_more_info1   => 'GN_INVENTORY_ITEM',
																																														p_more_info2   => gn_inventory_item,
																																														p_more_info3   => 'CONCAT_SEGS',
																																														p_more_info4   => l_concat_segs
																																														);
														END IF;
									ELSE
												write_log_prc  ('OPEN get_category_id_prc l_success  => False');

												OPEN get_category_id_prc (l_category_rec.structure_id, l_concat_segs);

												FETCH get_category_id_prc INTO gn_category_id;

												CLOSE get_category_id_prc;
									END IF;

									x_return_status := l_return_status;

					END LOOP;

     write_log_prc ('Procedure get_category_id_prc Ends....'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

	EXCEPTION
						WHEN NO_DATA_FOUND
						THEN
          write_log_prc ('Error in procedure get_category_id_prc:' || SQLERRM);
										l_messages := SQLERRM;
										xxd_common_utils.record_error (
																																									p_module       => 'INV',
																																									p_org_id       => gn_org_id,
																																									p_program      => 'Deckers Cost Elements Category Assignment Program',
																																									p_error_line   => SQLCODE,
																																									p_error_msg    => l_messages,
																																									p_created_by   => gn_user_id,
																																									p_request_id   => gn_conc_request_id,
																																									p_more_info1   => 'GN_INVENTORY_ITEM',
																																									p_more_info2   => gn_inventory_item,
																																									p_more_info3   => 'CONCAT_SEGS',
																																									p_more_info4   => l_concat_segs
																																									);
						WHEN OTHERS
						THEN
						    write_log_prc ('Error in procedure get_category_id_prc:' || SQLERRM);
										l_messages := SQLERRM;
										xxd_common_utils.record_error (
																																									p_module       => 'INV',
																																									p_org_id       => gn_org_id,
																																									p_program      => 'Deckers Cost Elements Category Assignment Program',
																																									p_error_line   => SQLCODE,
																																									p_error_msg    => l_messages,
																																									p_created_by   => gn_user_id,
																																									p_request_id   => gn_conc_request_id,
																																									p_more_info1   => 'GN_INVENTORY_ITEM',
																																									p_more_info2   => gn_inventory_item,
																																									p_more_info3   => 'CONCAT_SEGS',
																																									p_more_info4   => l_concat_segs
																																									);
END get_category_id_prc;

	/***************************************************************************
	-- FUNCTION validate_valueset_value_fn
	-- PURPOSE: This Function Validates the values sets 
	***************************************************************************/

	FUNCTION validate_valueset_value_fn (p_category_set_name         IN VARCHAR2,
																																						p_application_column_name   IN VARCHAR2,
																																						p_flex_value                IN VARCHAR2,
																																						p_flex_desc                 IN VARCHAR2)
	RETURN VARCHAR2
	AS
				x_rowid                VARCHAR2 (1000);
				ln_flex_value_id       NUMBER := 0;
				ln_flex_value_set_id   NUMBER := 0;
	   --ln_flex_value_id            NUMBER   := 0;
	BEGIN
	     write_log_prc ('Procedure validate_valueset_value_fn Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
				  write_log_prc ('validate_valueset_value_fn for '|| p_application_column_name	|| ' and value '|| p_flex_value);

						BEGIN
											SELECT ffs.flex_value_set_id, mcs.category_set_id
													INTO ln_flex_value_set_id, gn_category_set_id
													FROM fnd_id_flex_segments ffs, 
													     mtl_category_sets_v mcs              --, fnd_flex_values ffv
												WHERE 1 = 1
												  AND application_id = 401
														AND id_flex_code = 'MCAT'
														AND id_flex_num = mcs.structure_id       --l_structure_id
														AND ffs.enabled_flag = 'Y'
														-- AND ffv.enabled_flag        = 'Y'
														AND mcs.category_set_name = p_category_set_name --'TOPPS ITEM CATEGORY SET'
														-- AND ffs.flex_value_set_id = ffv.flex_value_set_id
														AND application_column_name = p_application_column_name;
				          -- AND flex_value = p_flex_value  ;
						EXCEPTION
								  	WHEN NO_DATA_FOUND
									  THEN
											   	ln_flex_value_set_id := 1;
									  WHEN OTHERS
									  THEN
												   ln_flex_value_set_id := 0;
						END;


						IF ln_flex_value_set_id IS NOT NULL
						THEN
										BEGIN
													SELECT flex_value_id
															INTO ln_flex_value_id
															FROM fnd_flex_values ffs
														WHERE 1 = 1
														  AND ln_flex_value_set_id = ffs.flex_value_set_id
																AND flex_value = p_flex_value;
									 EXCEPTION
											  	WHEN NO_DATA_FOUND
											  	THEN
														   	ln_flex_value_id := 0;
												  WHEN OTHERS
												  THEN
														    ln_flex_value_id := 0;
									  END;

											IF ln_flex_value_id = 0
											THEN
														RETURN 'E';
											ELSE
														RETURN 'S';
											END IF;
				  ELSE
							  RETURN 'S';
				  END IF;
						write_log_prc ('Procedure validate_valueset_value_fn Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
	EXCEPTION
				  WHEN NO_DATA_FOUND
				  THEN
						   	gn_record_error_flag := 1;
							   write_log_prc ('Error in function validate_valueset_value_fn:' || SQLERRM);
							   RETURN 'E';
				  WHEN OTHERS
				  THEN
						  	gn_record_error_flag := 1;
							  write_log_prc ('Error in function validate_valueset_value_fn:' || SQLERRM);
							  RETURN 'E';
	END validate_valueset_value_fn;

	/***************************************************************************
	-- PROCEDURE inv_category_validation_prc
	-- PURPOSE: This Procedure Validates the category values
	***************************************************************************/
	PROCEDURE inv_category_validation_prc (errbuf       OUT NOCOPY VARCHAR2,
																											  					    		retcode      OUT NOCOPY NUMBER)

	IS
				CURSOR cur_item_category
				IS
							SELECT *
									FROM xxdo.xxd_cst_duty_ele_cat_stg_t
								WHERE 1 = 1
								  AND record_status IN (gc_new_status); --,gc_error_status);

				--  l_errbuf    VARCHAR2(2000) := NULL;
				--  l_retcode   VARCHAR2(10)   := NULL;
				lc_err_msg              VARCHAR2 (2000) := NULL;
				x_return_status         VARCHAR2 (10) := NULL;
				l_category_set_exists   VARCHAR2 (10);
				l_old_category_id       NUMBER;
				l_segment_exists        VARCHAR2 (1);

	BEGIN
	     write_log_prc('Procedure inv_category_validation_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
				  OPEN cur_item_category;

				  LOOP
										FETCH cur_item_category
										BULK COLLECT INTO gt_item_cat_rec
										LIMIT 50;

							   EXIT WHEN gt_item_cat_rec.COUNT = 0;

									IF gt_item_cat_rec.COUNT > 0
									THEN
													-- Check if there are any records in the staging table that need to be processed
													FOR lc_item_cat_idx IN 1 .. gt_item_cat_rec.COUNT
													LOOP
																	gn_organization_id := NULL;
																	gn_inventory_item_id := NULL;
																	gn_category_id := NULL;
																	gn_category_set_id := NULL;
																	gc_err_msg := NULL;
																	gc_stg_tbl_process_flag := NULL;
																	gn_record_error_flag := 0;
																	lc_err_msg := NULL;
																	gn_inventory_item :=
																	gt_item_cat_rec (lc_item_cat_idx).item_number;
																	x_return_status := fnd_api.g_ret_sts_success;
																	l_segment_exists := 'Y';

													    -- Check if the mandatory field Organization code exists or not and validate the organization code
													    write_log_prc  ('gn_record_error_flag    => ' || gn_record_error_flag);


													    -- Validate value set values in Segments.
													    write_log_prc ( 'Validate value set values in Segments.');

																IF gt_item_cat_rec (lc_item_cat_idx).segment1 IS NOT NULL
																THEN
																			 x_return_status := 	validate_valueset_value_fn (
																																																																				p_category_set_name         => gt_item_cat_rec (lc_item_cat_idx).category_set_name,
																																																																				p_application_column_name   => 'SEGMENT1',
																																																																				p_flex_value                => gt_item_cat_rec (lc_item_cat_idx).segment1,
																																																																				p_flex_desc                 => gt_item_cat_rec (lc_item_cat_idx).segment1_desc
																																																																				);

																			 write_log_prc  ('Status of segment1 validation.' || x_return_status);

																				IF x_return_status = 'E'
																				THEN
																								l_segment_exists := 'N';
																								lc_err_msg := 'SEGMENT1 '|| gt_item_cat_rec (lc_item_cat_idx).segment1|| 'Not defind in Category Set'
																								                        || gt_item_cat_rec (lc_item_cat_idx).category_set_name;

																							 xxd_common_utils.record_error (
																																																							p_module       => 'INV',
																																																							p_org_id       => gn_org_id,
																																																							p_program      => 'Deckers Cost Elements Category Assignment Program',
																																																							p_error_line   => SQLCODE,
																																																							p_error_msg    => lc_err_msg,
																																																							p_created_by   => gn_user_id,
																																																							p_request_id   => gn_conc_request_id,
																																																							p_more_info1   => gt_item_cat_rec (lc_item_cat_idx).category_set_name,
																																																							p_more_info2   => gt_item_cat_rec (lc_item_cat_idx).item_number,
																																																							p_more_info3   => 'SEGMENT1',
																																																							p_more_info4   => gt_item_cat_rec (lc_item_cat_idx).segment1
																																																						);
																				END IF;
													-- ELSE
													--gn_record_error_flag := 1;
													   END IF;

													   write_log_prc  ('Validate value set values in Segment2.' || gn_record_error_flag);
													   x_return_status := fnd_api.g_ret_sts_success;

													  IF gt_item_cat_rec (lc_item_cat_idx).segment2 IS NOT NULL
													  THEN
																   x_return_status := validate_valueset_value_fn (
																																																															p_category_set_name         => gt_item_cat_rec (lc_item_cat_idx).category_set_name,
																																																															p_application_column_name   => 'SEGMENT2',
																																																															p_flex_value                => gt_item_cat_rec (lc_item_cat_idx).segment2,
																																																															p_flex_desc                 => gt_item_cat_rec (	lc_item_cat_idx).segment2_desc
																																																															);

																   write_log_prc ( 'Status of segment2 validation.' || x_return_status);

																			IF x_return_status = 'E'
																			THEN
																							l_segment_exists := 'N';
																							lc_err_msg := 'SEGMENT2 '|| gt_item_cat_rec (lc_item_cat_idx).segment2|| 'Not defind in Category Set'
																							                         || gt_item_cat_rec (lc_item_cat_idx).category_set_name;

																							xxd_common_utils.record_error (
																																																						p_module       => 'INV',
																																																						p_org_id       => gn_org_id,
																																																						p_program      => 'Deckers Cost Elements Category Assignment Program',
																																																						p_error_line   => SQLCODE,
																																																						p_error_msg    => lc_err_msg,
																																																						p_created_by   => gn_user_id,
																																																						p_request_id   => gn_conc_request_id,
																																																						p_more_info1   => gt_item_cat_rec (lc_item_cat_idx).category_set_name,
																																																						p_more_info2   => gt_item_cat_rec (lc_item_cat_idx).item_number,
																																																						p_more_info3   => 'SEGMENT2',
																																																						p_more_info4   => gt_item_cat_rec (lc_item_cat_idx).segment2
																																																						);
																			END IF;
													-- ELSE
													-- gn_record_error_flag := 1;
													  END IF;

															write_log_prc  ('Validate value set values in Segment3.'|| gn_record_error_flag);
															x_return_status := fnd_api.g_ret_sts_success;

															IF gt_item_cat_rec (lc_item_cat_idx).segment3 IS NOT NULL
															THEN
																		x_return_status := validate_valueset_value_fn (
																																																														p_category_set_name         => gt_item_cat_rec (lc_item_cat_idx).category_set_name,
																																																														p_application_column_name   => 'SEGMENT3',
																																																														p_flex_value                => gt_item_cat_rec (lc_item_cat_idx).segment3,
																																																														p_flex_desc                 => gt_item_cat_rec (lc_item_cat_idx).segment3_desc
																																																														);

																		write_log_prc  ('Status of segment3 validation.' || x_return_status);

																		IF x_return_status = 'E'
																		THEN
																					 l_segment_exists := 'N';
																					 lc_err_msg := 'SEGMENT3 '|| gt_item_cat_rec (lc_item_cat_idx).segment3|| 'Not defind in Category Set'
																						                         || gt_item_cat_rec (lc_item_cat_idx).category_set_name;

																						xxd_common_utils.record_error (
																																																					p_module       => 'INV',
																																																					p_org_id       => gn_org_id,
																																																					p_program      => 'Deckers Cost Elements Category Assignment Program',
																																																					p_error_line   => SQLCODE,
																																																					p_error_msg    => lc_err_msg,
																																																					p_created_by   => gn_user_id,
																																																					p_request_id   => gn_conc_request_id,
																																																					p_more_info1   => gt_item_cat_rec (lc_item_cat_idx).category_set_name,
																																																					p_more_info2   => gt_item_cat_rec (lc_item_cat_idx).item_number,
																																																					p_more_info3   => 'SEGMENT3',
																																																					p_more_info4   => gt_item_cat_rec (lc_item_cat_idx).segment3
																																																					);
																  END IF;
													  END IF;

													  write_log_prc  ('Validate value set values in Segment4.' || gn_record_error_flag);
													  x_return_status := fnd_api.g_ret_sts_success;

															IF gt_item_cat_rec (lc_item_cat_idx).segment4 IS NOT NULL
															THEN
																		 x_return_status := validate_valueset_value_fn (	
																																																															p_category_set_name         => gt_item_cat_rec (lc_item_cat_idx).category_set_name,
																																																															p_application_column_name   => 'SEGMENT4',
																																																															p_flex_value                => gt_item_cat_rec (	lc_item_cat_idx).segment4,
																																																															p_flex_desc                 => gt_item_cat_rec (	lc_item_cat_idx).segment4_desc
																																																															);

																   write_log_prc ( 'Status of segment4 validation.' || x_return_status);

																			IF x_return_status = 'E'
																			THEN
																						 l_segment_exists := 'N';
																						 lc_err_msg := 'SEGMENT4 '|| gt_item_cat_rec (lc_item_cat_idx).segment4|| 'Not defind in Category Set'
																							                         || gt_item_cat_rec (lc_item_cat_idx).category_set_name;

																			    xxd_common_utils.record_error (
																																																						p_module       => 'INV',
																																																						p_org_id       => gn_org_id,
																																																						p_program      => 'Deckers Cost Elements Category Assignment Program',
																																																						p_error_line   => SQLCODE,
																																																						p_error_msg    => lc_err_msg,
																																																						p_created_by   => gn_user_id,
																																																						p_request_id   => gn_conc_request_id,
																																																						p_more_info1   => gt_item_cat_rec (lc_item_cat_idx).category_set_name,
																																																						p_more_info2   => gt_item_cat_rec (lc_item_cat_idx).item_number,
																																																						p_more_info3   => 'SEGMENT4',
																																																						p_more_info4   => gt_item_cat_rec (lc_item_cat_idx).segment4
																																																						);
																   END IF;
													  END IF;

													  write_log_prc  ('x_return_status         =>' || x_return_status);
												   write_log_prc  ('gn_record_error_flag    =>' || gn_record_error_flag);
													  write_log_prc  ('p_batch_number          =>' || gt_item_cat_rec (lc_item_cat_idx).batch_number);
													  write_log_prc  ('record_id               =>' || gt_item_cat_rec (lc_item_cat_idx).record_id);

															IF l_segment_exists = 'N'
															THEN
																		 UPDATE xxd_inv_item_cat_stg_t
																			 		SET record_status = gc_validate_status
																		 	WHERE record_id = gt_item_cat_rec (lc_item_cat_idx).record_id;
															ELSE
																		 UPDATE xxd_inv_item_cat_stg_t
																			 		SET record_status = gc_error_status,
																				 					error_message = 'ValueSet Validation Error'
																			 WHERE record_id =	gt_item_cat_rec (lc_item_cat_idx).record_id;
															END IF;
										END LOOP;
							END IF;

							COMMIT;
				END LOOP;

				CLOSE cur_item_category;
				write_log_prc('Procedure inv_category_validation_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
	EXCEPTION
				  WHEN OTHERS
				  THEN
							   write_log_prc ('Error in Procedure inv_category_validation_prc:' || SQLERRM);
							   errbuf := SQLERRM;
				   			retcode := 2;
							   lc_err_msg := 'Unexpected error while cursor fetching into PL/SQL table - '	|| SQLERRM;
							   write_log_prc (gc_debug_flag||'-'||lc_err_msg);
							   xxd_common_utils.record_error (
																																									p_module       => 'INV',
																																									p_org_id       => gn_org_id,
																																									p_program      => 'Deckers Cost Elements Category Assignment Program',
																																									p_error_line   => SQLCODE,
																																									p_error_msg    => lc_err_msg,
																																									p_created_by   => gn_user_id,
																																									p_request_id   => gn_conc_request_id,
																																									p_more_info1   => NULL
																																									);
	END inv_category_validation_prc;

	/***************************************************************************
	-- PROCEDURE create_category_assignment_prc
	-- PURPOSE: This Procedure creates assignment for the category
	***************************************************************************/

	PROCEDURE create_category_assignment_prc (
																																											p_category_id         IN     NUMBER,
																																											p_category_set_id     IN     NUMBER,
																																											p_inventory_item_id   IN     NUMBER,
																																											p_organization_id     IN     NUMBER,
																																											x_return_status          OUT VARCHAR2
																																											)
	AS
			lx_return_status   NUMBER;
			x_error_message    VARCHAR2 (2000);
			--x_return_status       VARCHAR2 (10);
			x_msg_data         VARCHAR2 (2000);
			li_msg_count       NUMBER;
			ls_msg_data        VARCHAR2 (4000);
			l_messages         VARCHAR2 (4000);
			li_error_code      NUMBER;
			x_message_list     error_handler.error_tbl_type;
			ln_rec_cnt         NUMBER := 0;

BEGIN
			  write_log_prc ( 'Procedure create_category_assignment_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

					SELECT COUNT (1)
							INTO ln_rec_cnt
							FROM mtl_item_categories
						WHERE 1 = 1
						  AND inventory_item_id = p_inventory_item_id
								AND organization_id = p_organization_id
								AND category_set_id = p_category_set_id
								AND category_id = NVL (p_category_id, 0);

					IF ln_rec_cnt = 0
					THEN
								 inv_item_category_pub.create_category_assignment (
																																																											p_api_version         => 1,
																																																											p_init_msg_list       => fnd_api.g_false,
																																																											p_commit              => fnd_api.g_false,
																																																											x_return_status       => x_return_status,
																																																											x_errorcode           => li_error_code,
																																																											x_msg_count           => li_msg_count,
																																																											x_msg_data            => ls_msg_data,
																																																											p_category_id         => p_category_id,
																																																											p_category_set_id     => p_category_set_id,
																																																											p_inventory_item_id   => p_inventory_item_id,
																																																											p_organization_id     => p_organization_id
																																																											);

						   --  error_handler.get_message_list (x_message_list => x_message_list);

								 IF x_return_status <> fnd_api.g_ret_sts_success
					    THEN
													write_log_prc  ('status is count:' || x_message_list.COUNT);
													write_log_prc  ('status is 1:' || x_return_status);

													fnd_msg_pub.count_and_get (p_encoded   => 'F',
																																								p_count     => li_msg_count,
																																								p_data      => ls_msg_data);

													write_log_prc  ('status is ls_msg_data 1:' || ls_msg_data);

													FOR k IN 1 .. li_msg_count
													LOOP
																	l_messages := l_messages|| fnd_msg_pub.get (p_msg_index => k, p_encoded => 'F')|| ';';
																	write_log_prc  ('l_messages => ' || l_messages);
																	fnd_msg_pub.delete_msg (k);
													END LOOP;

													xxd_common_utils.record_error (
																																												p_module       => 'INV',
																																												p_org_id       => gn_org_id,
																																												p_program      => 'Deckers Cost Elements Category Assignment Program',
																																												p_error_line   => SQLCODE,
																																												p_error_msg    => NVL (SUBSTR (l_messages, 2000),	'Error in create_category_assignment_prc'),
																																												p_created_by   => gn_user_id,
																																												p_request_id   => gn_conc_request_id,
																																												p_more_info1   => gn_inventory_item,
																																												p_more_info2   => 'p_category_id',
																																												p_more_info3   => p_category_id,
																																												p_more_info4   => p_category_set_id
																																												);
									END IF;
			  ELSE
									x_return_status := fnd_api.g_ret_sts_error;
									l_messages :=	'An item '|| gn_inventory_item|| ' can be assigned to only one category within this category set.';
									xxd_common_utils.record_error (
																																								p_module       => 'INV',
																																								p_org_id       => gn_org_id,
																																								p_program      => 'Deckers Cost Elements Category Assignment Program',
																																								p_error_line   => SQLCODE,
																																								p_error_msg    => NVL (SUBSTR (l_messages, 2000),'Error in create_category_assignment_prc'),
																																								p_created_by   => gn_user_id,
																																								p_request_id   => gn_conc_request_id,
																																								p_more_info1   => gn_inventory_item,
																																								p_more_info2   => 'p_category_id',
																																								p_more_info3   => p_category_id,
																																								p_more_info4   => p_category_set_id
																																								);
			  END IF;                                                  --ln_rec_cnt >0

			  write_log_prc  ('status is 2:' || x_return_status);
			  write_log_prc  ('Processing category  Status ' || x_return_status);
			  write_log_prc ( 'Procedure create_category_assignment_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

 EXCEPTION
			   WHEN NO_DATA_FOUND
			   THEN
						    write_log_prc ('Error in procedure create_category_assignment_prc:' || SQLERRM);
						    l_messages := SQLERRM;
										xxd_common_utils.record_error (
																																									p_module       => 'INV',
																																									p_org_id       => gn_org_id,
																																									p_program      => 'Deckers Cost Elements Category Assignment Program',
																																									p_error_line   => SQLCODE,
																																									p_error_msg    => NVL (SUBSTR (l_messages, 2000),'Error in create_category_assignment_prc'),
																																									p_created_by   => gn_user_id,
																																									p_request_id   => gn_conc_request_id,
																																									p_more_info1   => gn_inventory_item,
																																									p_more_info2   => 'p_category_id',
																																									p_more_info3   => p_category_id,
																																									p_more_info4   => p_category_set_id
																																									);
						WHEN OTHERS
						THEN
						    write_log_prc ('Error in procedure create_category_assignment_prc:' || SQLERRM);
						    l_messages := SQLERRM;
										xxd_common_utils.record_error (
																																									p_module       => 'INV',
																																									p_org_id       => gn_org_id,
																																									p_program      => 'Deckers Cost Elements Category Assignment Program',
																																									p_error_line   => SQLCODE,
																																									p_error_msg    => NVL (SUBSTR (l_messages, 2000),
																																																																'Error in create_category_assignment_prc'),
																																									p_created_by   => gn_user_id,
																																									p_request_id   => gn_conc_request_id,
																																									p_more_info1   => gn_inventory_item,
																																									p_more_info2   => 'p_category_id',
																																									p_more_info3   => p_category_id,
																																									p_more_info4   => p_category_set_id
																																									);
 END create_category_assignment_prc;

	/**************************************************************************
	-- PROCEDURE update_category_assignment_prc
	-- PURPOSE: This Procedure udpates assignment for the category
	***************************************************************************/

	PROCEDURE update_category_assignment_prc (
																																											p_category_id             mtl_categories_b.category_id%TYPE,
																																											p_old_category_id         mtl_categories_b.category_id%TYPE,
																																											p_category_set_id         mtl_category_sets_tl.category_set_id%TYPE,
																																											p_inventory_item_id       mtl_system_items_b.inventory_item_id%TYPE,
																																											p_organization_id         mtl_parameters.organization_id%TYPE,
																																											x_return_status       OUT VARCHAR2
																																											)
	AS
			-- lx_return_status      NUMBER;
			x_error_message   VARCHAR2 (2000);
			--x_return_status       VARCHAR2 (10);
			x_msg_data        VARCHAR2 (2000);
			li_msg_count      NUMBER;
			ls_msg_data       VARCHAR2 (4000);
			l_messages        VARCHAR2 (4000);
			li_error_code     NUMBER;
			x_message_list    error_handler.error_tbl_type;

	BEGIN
				  write_log_prc  ('Procedure update_category_assignment_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

				  inv_item_category_pub.update_category_assignment (
																																																								p_api_version         => 1.0,
																																																								p_init_msg_list       => fnd_api.g_false,
																																																								p_commit              => fnd_api.g_false,
																																																								p_category_id         => p_category_id,
																																																								p_old_category_id     => p_old_category_id,
																																																								p_category_set_id     => p_category_set_id,
																																																								p_inventory_item_id   => p_inventory_item_id,
																																																								p_organization_id     => p_organization_id,
																																																								x_return_status       => x_return_status,
																																																								x_errorcode           => li_error_code,
																																																								x_msg_count           => li_msg_count,
																																																								x_msg_data            => x_msg_data
																																																								);

						IF (x_return_status <> fnd_api.g_ret_sts_success)
						THEN
									 error_handler.get_message_list (x_message_list => x_message_list);
									 l_messages := NULL;

										FOR i IN 1 .. x_message_list.COUNT
										LOOP
														IF l_messages IS NULL
														THEN
																	l_messages := x_message_list (i).MESSAGE_TEXT;
														ELSE
																	l_messages :=
																				l_messages || ' ' || x_message_list (i).MESSAGE_TEXT;
														END IF;

													fnd_msg_pub.delete_msg (i);
										END LOOP;

										write_log_prc  ('Error Messages (Update Item Category Assignment):'|| l_messages);

										xxd_common_utils.record_error (
																																									p_module       => 'INV',
																																									p_org_id       => gn_org_id,
																																									p_program      => 'Deckers Cost Elements Category Assignment Program',
																																									p_error_line   => SQLCODE,
																																									p_error_msg    => NVL (SUBSTR (l_messages, 2000),'Error in create_category_assignment_prc'),
																																									p_created_by   => gn_user_id,
																																									p_request_id   => gn_conc_request_id,
																																									p_more_info1   => 'update_category_assignment_prc',
																																									p_more_info2   => gn_inventory_item,
																																									p_more_info3   => p_category_id,
																																									p_more_info4   => p_category_set_id
																																									);
						END IF;
	 				write_log_prc  ('Procedure update_category_assignment_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

		EXCEPTION
						 WHEN NO_DATA_FOUND
				 		THEN
							    write_log_prc ('Error in procedure update_category_assignment_prc:' || SQLERRM);
				    			l_messages := SQLERRM;

							    xxd_common_utils.record_error (
																																										p_module       => 'INV',
																																										p_org_id       => gn_org_id,
																																										p_program      => 'Deckers Cost Elements Category Assignment Program',
																																										p_error_line   => SQLCODE,
																																										p_error_msg    => NVL (SUBSTR (l_messages, 2000),'Error in update_category_assignment_prc'),
																																										p_created_by   => gn_user_id,
																																										p_request_id   => gn_conc_request_id,
																																										p_more_info1   => 'update_category_assignment_prc',
																																										p_more_info2   => gn_inventory_item,
																																										p_more_info3   => p_category_id,
																																										p_more_info4   => p_category_set_id
																																										);
				   WHEN OTHERS
			   	THEN
							    write_log_prc ('Error in procedure update_category_assignment_prc:' || SQLERRM);
							    l_messages := SQLERRM;
							    xxd_common_utils.record_error (
																																										p_module       => 'INV',
																																										p_org_id       => gn_org_id,
																																										p_program      => 'Deckers Cost Elements Category Assignment Program',
																																										p_error_line   => SQLCODE,
																																										p_error_msg    => NVL (SUBSTR (l_messages, 2000),'Error in update_category_assignment_prc'),
																																										p_created_by   => gn_user_id,
																																										p_request_id   => gn_conc_request_id,
																																										p_more_info1   => 'update_category_assignment_prc',
																																										p_more_info2   => gn_inventory_item,
																																										p_more_info3   => p_category_id,
																																										p_more_info4   => p_category_set_id
																																										);
	END update_category_assignment_prc;

	/**************************************************************************
	-- PROCEDURE cat_assignment_child_prc
	-- PURPOSE: This Procedure calls the create assignment/update assignment procedures
	--          with the required values
	***************************************************************************/

	PROCEDURE cat_assignment_child_prc (errbuf              OUT NOCOPY VARCHAR2,
																																					retcode             OUT NOCOPY NUMBER,
																																					pn_batch_number   IN            NUMBER)
	IS
			lc_err_msg              VARCHAR2 (2000) := NULL;
			x_return_status         VARCHAR2 (10) := NULL;
			l_category_set_exists   VARCHAR2 (10);
			l_old_category_id       NUMBER;
			l_segment_exists        VARCHAR2 (1);
			gn_organization_code    VARCHAR2 (30);
			xv_errbuf               VARCHAR2 (300);
			xn_retcode              NUMBER (10);
			ln_count                NUMBER (10);
			ln_success_count        NUMBER (20) := 0;
			ln_error_count          NUMBER (20) := 0;

			CURSOR cur_item_category (p_batch NUMBER)
			IS
					SELECT *
							FROM xxdo.xxd_cst_duty_ele_cat_stg_t
						WHERE 1 = 1
						  AND record_status = gc_new_status 
								AND batch_number = p_batch;

			CURSOR get_structure_id (cp_category_set_name VARCHAR2)
			IS
						SELECT category_set_id
								FROM mtl_category_sets_v
							WHERE 1 = 1
							  AND category_set_name = cp_category_set_name;

	BEGIN
	     write_log_prc ('Procedure cat_assignment_child_program_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
			  	--gc_debug_flag := p_debug;

				  OPEN cur_item_category (pn_batch_number);

				  LOOP
										FETCH cur_item_category
										BULK COLLECT INTO gt_item_cat_rec
										LIMIT 500;

						   	EXIT WHEN gt_item_cat_rec.COUNT = 0;

										IF gt_item_cat_rec.COUNT > 0
										THEN
													 -- Check if there are any records in the staging table that need to be processed
														FOR lc_item_cat_idx IN 1 .. gt_item_cat_rec.COUNT
														LOOP
																		gn_organization_id := NULL;
																		gn_inventory_item_id := NULL;
																		gn_category_id := NULL;
																		gn_category_set_id := NULL;
																		gc_err_msg := NULL;
																		gc_stg_tbl_process_flag := NULL;
																		gn_record_error_flag := 0;
																	 gn_inventory_item := gt_item_cat_rec (lc_item_cat_idx).item_number;
																		x_return_status := fnd_api.g_ret_sts_success;
																		l_segment_exists := 'Y';

													     get_category_id_prc ( p_processing_row_id   => gt_item_cat_rec (lc_item_cat_idx).record_id,
																                        x_return_status       => x_return_status);


													     OPEN get_structure_id (cp_category_set_name => gt_item_cat_rec (lc_item_cat_idx).category_set_name);

													     FETCH get_structure_id INTO gn_category_set_id;

													     CLOSE get_structure_id;

																		IF gn_category_id IS NULL
																		THEN
																					 gn_record_error_flag := 1;
																		ELSE
																      gn_organization_id :=	gt_item_cat_rec (lc_item_cat_idx).organization_id;

																      gn_inventory_item_id := gt_item_cat_rec (lc_item_cat_idx).inventory_item_id;

                      write_log_prc  ('gn_inventory_item_id    => ' || gn_inventory_item_id);
																      write_log_prc  ('gn_organization_id    => ' || gn_organization_id);
																      write_log_prc  ('gn_category_set_id    => ' || gn_category_set_id);

																						IF gn_organization_id IS NOT NULL AND gn_inventory_item_id IS NOT NULL
																						THEN
																										BEGIN
																													SELECT category_id, 'Y'
																															INTO l_old_category_id, l_category_set_exists
																															FROM mtl_item_categories
																														WHERE inventory_item_id = gn_inventory_item_id
																																AND organization_id = gn_organization_id
																																AND category_set_id = gn_category_set_id;
																										EXCEPTION
																												  	WHEN NO_DATA_FOUND
																												  	THEN
																																   l_category_set_exists := 'N';
																										END;

																			       write_log_prc  ('gn_category_id    => ' || gn_category_id);

																										IF l_category_set_exists = 'N'
																										THEN
																														BEGIN
																																			create_category_assignment_prc (
																																																															p_category_id         => gn_category_id,
																																																															p_category_set_id     => gn_category_set_id,
																																																															p_inventory_item_id   => gn_inventory_item_id,
																																																															p_organization_id     => gn_organization_id,
																																																															x_return_status       => x_return_status
																																																															);
																														EXCEPTION
																																	  WHEN OTHERS
																																	  THEN
																																				   write_log_prc  ('Error While Creating Category Assignment'|| SQLERRM);
																														END;

																										ELSE
																														BEGIN
																																			update_category_assignment_prc (
																																																															p_category_id         => gn_category_id,
																																																															p_old_category_id     => l_old_category_id,
																																																															p_category_set_id     => gn_category_set_id,
																																																															p_inventory_item_id   => gn_inventory_item_id,
																																																															p_organization_id     => gn_organization_id,
																																																															x_return_status       => x_return_status
																																																															);
																														EXCEPTION
																																	  WHEN OTHERS
																																	  THEN
																																				   write_log_prc  ('Error While Updating Category Assignment'	|| SQLERRM);
																														END;
																			       END IF;
																      END IF;
													     END IF;

													     write_log_prc ( 'x_return_status         =>' || x_return_status);
													     write_log_prc ( 'gn_record_error_flag    =>' || gn_record_error_flag);
													     --print_msg_prc(gc_debug_flag,'p_batch_number                =>'||gt_item_cat_rec(lc_item_cat_idx).batch_number );
													     write_log_prc  ('record_id       =>'|| gt_item_cat_rec (lc_item_cat_idx).record_id);

																	IF x_return_status = 'S'
																	THEN
																				 ln_success_count := ln_success_count + 1;

																					UPDATE xxd_inv_item_cat_stg_t
																								SET record_status = gc_process_status
																						WHERE record_id = gt_item_cat_rec (lc_item_cat_idx).record_id;

													    ELSE
																     ln_error_count := ln_error_count + 1;
																     write_log_prc  ('gn_inventory_item_id    =>' || gn_inventory_item_id ||
																					                ' organization_id =>     =>' || gt_item_cat_rec (lc_item_cat_idx).organization_id||
																			                  ' gt_item_cat_rec(lc_item_cat_idx).record_id => '||gt_item_cat_rec (lc_item_cat_idx).record_id);

																					UPDATE xxd_inv_item_cat_stg_t
																								SET record_status = gc_error_status
																						WHERE record_id =	gt_item_cat_rec (lc_item_cat_idx).record_id;

																					ln_count := SQL%ROWCOUNT;
																					write_log_prc  ('gn_inventory_item_id    =>'|| gn_inventory_item_id||
																								        ' organization_id        =>'|| gt_item_cat_rec (lc_item_cat_idx).organization_id||
																								        ' gt_item_cat_rec(lc_item_cat_idx).record_id => '|| gt_item_cat_rec (lc_item_cat_idx).record_id);

																     write_log_prc  ('ln_count    =>' || ln_count);
													    END IF;
										    END LOOP;
							   END IF;

							   COMMIT;
				  END LOOP;

				  CLOSE cur_item_category;

				  COMMIT;

				  retcode := 0;
      write_log_prc ('Procedure cat_assignment_child_program_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

	EXCEPTION
			   	WHEN OTHERS
				   THEN
							    write_log_prc ('Error in procedure cat_assignment_child_program_prc:' || SQLERRM);
							    retcode := 2;
	END cat_assignment_child_prc;	

	/**************************************************************************
	-- PROCEDURE inv_category_load_prc
	-- PURPOSE: This Procedure perfom category valdiations and upload into ORACLE
	***************************************************************************/
	PROCEDURE inv_category_load_prc (errbuf                   OUT NOCOPY VARCHAR2,
																																		retcode                  OUT NOCOPY NUMBER,
																																		pv_category_set_name   IN            VARCHAR2)

	IS
			CURSOR cur_item_category (p_group_id NUMBER)
			IS
					SELECT *
							FROM xxdo.xxd_cst_duty_ele_cat_stg_t
						WHERE 1 = 1
						  AND record_status = gc_new_status 
								AND group_id = p_group_id;

			CURSOR c_batch_num
			IS
					SELECT DISTINCT batch_number 
					  FROM          xxdo.xxd_cst_duty_ele_cat_stg_t;

			ln_batch_num            NUMBER; -- c_batch_num%rowtype;

			CURSOR c_batch (p_batch_size NUMBER)
			IS
						SELECT record_id,
													NTILE (p_batch_size)
																OVER (ORDER BY
																									category_set_name,
																									segment1,
																									segment2,
																									segment3,
																									segment4) batch_num
								FROM xxdo.xxd_cst_duty_ele_cat_stg_t
							WHERE 1 = 1
							  AND batch_number IS NULL AND record_status = gc_new_status;

			TYPE t_batch_type IS TABLE OF c_batch%ROWTYPE
																											INDEX BY BINARY_INTEGER;

			t_batch_tab             t_batch_type;

			lc_err_msg              VARCHAR2 (2000) := NULL;
			x_return_status         VARCHAR2 (10) := NULL;
			l_category_set_exists   VARCHAR2 (10);
			l_old_category_id       NUMBER;
			l_segment_exists        VARCHAR2 (1);
			gn_organization_code    VARCHAR2 (30);
			xv_errbuf               VARCHAR2 (300);
			xn_retcode              NUMBER (10);
			ln_count                NUMBER (10);
			ln_success_count        NUMBER (20) := 0;
			ln_error_count          NUMBER (20) := 0;
			l_group_id              NUMBER;
			p_batch_size            NUMBER := 10;
			ln_cntr                 NUMBER := 0;
			lc_message              VARCHAR2 (200);
			ln_loop                 NUMBER;

			TYPE hdr_batch_id_t IS TABLE OF NUMBER
																													INDEX BY BINARY_INTEGER;

			ln_hdr_batch_id         hdr_batch_id_t;

			lc_phase                VARCHAR2 (200);
			lc_status               VARCHAR2 (200);
			ln_batch_cnt            NUMBER;
			ln_valid_rec_cnt        NUMBER;
			ln_parent_request_id    NUMBER := fnd_global.conc_request_id;

			TYPE request_table IS TABLE OF NUMBER
																												INDEX BY BINARY_INTEGER;

			l_req_id                request_table;

			ln_request_id           NUMBER;
			lc_dev_phase            VARCHAR2 (200);
			lc_dev_status           VARCHAR2 (200);
			lb_wait                 BOOLEAN;
			lv_errbuf               VARCHAR2 (4000);
			lv_retcode              VARCHAR2 (100);


	BEGIN 
						write_log_prc ('Procedure inv_category_load_prc Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
						--gc_debug_flag := p_debug;

						write_log_prc ('Insert succesful records from SKU Table to Category Table');

						insert_cat_to_stg_prc (lv_errbuf
																												,lv_retcode);
						BEGIN
											SELECT DISTINCT group_id
													INTO l_group_id
													FROM xxdo.xxd_cst_duty_ele_cat_stg_t
												WHERE 1 = 1
												  AND category_set_name = pv_category_set_name;

											fnd_file.put_line (fnd_file.output, 'Group Id for this run is - ' || l_group_id);

						EXCEPTION
											WHEN NO_DATA_FOUND
											THEN
															write_log_prc ( 'No Records found to process. ');
															l_group_id := NULL;
											WHEN OTHERS
											THEN
															write_log_prc ( 'Error while getting group id for this run - ');
															l_group_id := 0;
						END;

						IF l_group_id IS NOT NULL
						THEN
										SELECT COUNT (*)
												INTO ln_valid_rec_cnt
												FROM xxdo.xxd_cst_duty_ele_cat_stg_t
											WHERE 1 = 1
											  AND batch_number IS NULL
													AND record_status = gc_new_status
													AND group_id = l_group_id;

									/*  FOR i IN 1 .. p_batch_size
											LOOP
														BEGIN
																	SELECT XXTOP_ITEM_CATEGORIES_BATCH_S.NEXTVAL
																			INTO ln_hdr_batch_id (i)
																			FROM DUAL;

																	print_msg_prc (
																				gc_debug_flag,
																				'ln_hdr_batch_id(i) := ' || ln_hdr_batch_id (i));
														EXCEPTION
																	WHEN OTHERS
																	THEN
																				ln_hdr_batch_id (i + 1) := ln_hdr_batch_id (i) + 1;
														END;

														print_msg_prc (gc_debug_flag,
																													' ln_valid_rec_cnt := ' || ln_valid_rec_cnt);
														print_msg_prc (
																	gc_debug_flag,
																				'ceil( ln_valid_rec_cnt/p_batch_size) := '
																	|| CEIL (ln_valid_rec_cnt / p_batch_size));

														UPDATE XXD_INV_ITEM_CAT_STG_T
																	SET batch_number = ln_hdr_batch_id (i),
																					conc_request_id = ln_parent_request_id
															WHERE     batch_number IS NULL
																					AND ROWNUM <= CEIL (ln_valid_rec_cnt / p_batch_size)
																					AND RECORD_STATUS = gc_new_status;
											END LOOP;*/

										OPEN c_batch (p_batch_size);

										write_log_prc ( 'p_batch_size ' || p_batch_size);

										LOOP
														t_batch_tab.delete;

														FETCH c_batch
														BULK COLLECT INTO t_batch_tab
														LIMIT 5000;

														EXIT WHEN t_batch_tab.COUNT = 0;
														write_log_prc  (' t_batch_tab.COUNT' || t_batch_tab.COUNT);

														FOR i IN 1 .. t_batch_tab.COUNT
														LOOP
																		BEGIN
																					UPDATE xxdo.xxd_cst_duty_ele_cat_stg_t
																								SET batch_number = TO_NUMBER (t_batch_tab (i).batch_num),
																												conc_request_id = ln_parent_request_id
																						WHERE record_id = t_batch_tab (i).record_id;

																					COMMIT;
																		EXCEPTION
																							WHEN OTHERS
																							THEN
																											write_log_prc (' Update Batch Number Exception' || SQLERRM);
																		END;

																		write_log_prc  (' t_batch_tab(i).batch_num' || t_batch_tab (i).batch_num);
																		write_log_prc  ('t_batch_tab (i).record_id ' || t_batch_tab (i).record_id);
														END LOOP;

														COMMIT;

										END LOOP;

										CLOSE c_batch;

										COMMIT;

										UPDATE xxdo.xxd_cst_duty_ele_cat_stg_t x2
													SET x2.batch_number = (SELECT MIN (batch_number)
																																						FROM xxdo.xxd_cst_duty_ele_cat_stg_t x1
																																					WHERE 1 = 1
																																					  AND NVL (x1.segment1, 'XX') =	NVL (x2.segment1, 'XX')
																																							AND NVL (x1.segment2, 'XX') =	NVL (x2.segment2, 'XX')
																																							AND NVL (x1.segment3, 'XX') = NVL (x2.segment3, 'XX')
																																							AND NVL (x1.segment4, 'XX') = NVL (x2.segment4, 'XX') 
																																			 ) --  WHERE CATEGORY_SET_NAME = 'TARRIF CODE'
																																																											;

									COMMIT;
									ln_loop := 1;

									OPEN c_batch_num;

									LOOP
													FETCH c_batch_num INTO ln_batch_num;

													EXIT WHEN c_batch_num%NOTFOUND;

													ln_hdr_batch_id (ln_loop) := ln_batch_num;
													write_log_prc ( 'ln_hdr_batch_id(ln_loop) ' || ln_hdr_batch_id (ln_loop));

													ln_loop := ln_loop + 1;
									END LOOP;

									CLOSE c_batch_num;

									COMMIT;

									FOR l IN 1 .. ln_hdr_batch_id.COUNT
									LOOP
													SELECT COUNT (*)
															INTO ln_cntr
															FROM xxdo.xxd_cst_duty_ele_cat_stg_t
														WHERE 1 = 1
														  AND record_status = gc_new_status
																AND batch_number = ln_hdr_batch_id (l);

													write_log_prc ( 'ln_cntr ' || ln_cntr);

													IF ln_cntr > 0
													THEN
																	BEGIN
																						ln_request_id := apps.fnd_request.submit_request (
																																																																								'XXDO',
																																																																								'XXD_CST_ELE_CAT_CHILD_PRG',
																																																																								'',
																																																																								'',
																																																																								FALSE,
																																																																								ln_hdr_batch_id (l)
  																																																																		--		'N'
																																																																								);

																						write_log_prc  ('v_request_id := ' || ln_request_id);

																						IF ln_request_id > 0
																						THEN
																										l_req_id (l) := ln_request_id;
																										COMMIT;
																						ELSE
																									ROLLBACK;
																						END IF;

																	EXCEPTION
																						WHEN NO_DATA_FOUND
																						THEN
																										retcode := 2;
																										errbuf := errbuf || SQLERRM;
																										write_log_prc ( 'Calling WAIT FOR REQUEST XXD_CST_ELE_CAT_CHILD_PRG error'|| SQLERRM);

																						WHEN OTHERS
																						THEN
																										retcode := 2;
																										errbuf := errbuf || SQLERRM;
																										write_log_prc ('Calling WAIT FOR REQUEST XXD_CST_ELE_CAT_CHILD_PRG error'|| SQLERRM);
																	END;
													END IF;
									END LOOP;

									write_log_prc ('Calling WAIT FOR REQUEST XXD_CST_ELE_CAT_CHILD_PRG to complete');

									IF l_req_id.COUNT > 0
									THEN
													FOR rec IN l_req_id.FIRST .. l_req_id.LAST
													LOOP
																	IF l_req_id (rec) > 0
																	THEN
																					LOOP
																									lc_dev_phase := NULL;
																									lc_dev_status := NULL;
																									lb_wait := fnd_concurrent.wait_for_request (
																																																																					request_id   => l_req_id (rec), --ln_concurrent_request_id
																																																																					interval     => 1,
																																																																					max_wait     => 1,
																																																																					phase        => lc_phase,
																																																																					status       => lc_status,
																																																																					dev_phase    => lc_dev_phase,
																																																																					dev_status   => lc_dev_status,
																																																																					MESSAGE      => lc_message
																																																																					);

																									IF ((UPPER (lc_dev_phase) = 'COMPLETE')	OR (UPPER (lc_phase) = 'COMPLETED'))
																									THEN
																													EXIT;
																									END IF;
																					END LOOP;
																	END IF;
													END LOOP;
									END IF;


									SELECT COUNT (*)
											INTO ln_success_count
											FROM xxdo.xxd_cst_duty_ele_cat_stg_t
										WHERE 1 = 1
										  AND group_id = l_group_id 
												AND record_status = 'P';

									SELECT COUNT (*)
											INTO ln_error_count
											FROM xxdo.xxd_inv_item_cat_stg_t
										WHERE group_id = l_group_id 
										  AND record_status = 'E';


									apps.fnd_file.put_line(apps.fnd_file.output,'                                                                      Deckers Cost Elements Category Assignment Program ');
									apps.fnd_file.put_line(apps.fnd_file.output,'');
									apps.fnd_file.put_line(apps.fnd_file.output,'Date:'||TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
									apps.fnd_file.put_line(apps.fnd_file.output,'');
									apps.fnd_file.put_line(apps.fnd_file.output,'');
									apps.fnd_file.put_line(apps.fnd_file.output,'');
									fnd_file.put_line (fnd_file.output,	'=========================== Total Summary ============================');
									fnd_file.put_line (fnd_file.output, 'Total Number of records successfully processed ==> '|| ln_success_count);
									fnd_file.put_line (fnd_file.output,	'Total Number of Error records                  ==> '|| ln_error_count);
									fnd_file.put_line (fnd_file.output,	'======================================================================');
						ELSE
									fnd_file.put_line (fnd_file.output, 'No Records to process');
						END IF;

					 write_log_prc ('Procedure inv_category_load_prc Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

	EXCEPTION
						WHEN OTHERS
						THEN
										write_log_prc ('Error in procedure inv_category_load_prc:' || SQLERRM);
										errbuf := SQLERRM;
										retcode := 2;
										lc_err_msg := 'Unexpected error while cursor fetching into PL/SQL table - '|| SQLERRM;
										xxd_common_utils.record_error (
																																									p_module       => 'INV',
																																									p_org_id       => gn_org_id,
																																									p_program      => 'Deckers Cost Elements Category Assignment Program',
																																									p_error_line   => SQLCODE,
																																									p_error_msg    => lc_err_msg,
																																									p_created_by   => gn_user_id,
																																									p_request_id   => gn_conc_request_id,
																																									p_more_info1   => NULL
																																									);
	END inv_category_load_prc;

/**********************************************************************************
-- PROCEDURE purge_xxd_cst_duty_ele_inb_prc
-- PURPOSE: This Procedure Purge the Data from XXD_CST_DUTY_ELE_INB_STG_TR_T Table
***********************************************************************************/
PROCEDURE purge_xxd_cst_duty_ele_inb_prc (errbuf          OUT NOCOPY    VARCHAR2
																																									,retcode         OUT NOCOPY    VARCHAR2
																																									,pv_num_days                IN NUMBER)
IS
  ln_days    NUMBER:=0;
		BEGIN
		     ln_days := NVL (pv_num_days, 0);
		     write_log_prc ('Number Of Days to Retain, Set to: '||ln_days||' Days');

							write_log_prc ('Delete XXD_CST_DUTY_ELE_INB_STG_TR_T Table Data Based on Purge Parameter:');
							write_log_prc ('-------------------------------------------------------------------------');

							BEGIN

												DELETE xxdo.xxd_cst_duty_ele_inb_stg_tr_t
													WHERE TRUNC (creation_date) <= (TRUNC (sysdate) - ln_days);

							     write_log_prc (SQL%ROWCOUNT||' Rows Deleted from XXDO.XXD_CST_DUTY_ELE_INB_STG_TR_T Table');

							EXCEPTION
							     WHEN OTHERS
												THEN
												    write_log_prc ('Failed to Delete Records from XXDO.XXD_CST_DUTY_ELE_INB_STG_TR_T Table');
							END;
END purge_xxd_cst_duty_ele_inb_prc;


/**********************************************************************************
-- PROCEDURE purge_xxd_cst_duty_ele_upld_stg_t_prc
-- PURPOSE: This Procedure Purge the Data from xxd_cst_duty_ele_upld_stg_t Table
***********************************************************************************/
PROCEDURE purge_xxd_cst_duty_ele_upld_prc (errbuf          OUT NOCOPY    VARCHAR2
																																										,retcode         OUT NOCOPY    VARCHAR2
																																										,pv_num_days                IN NUMBER)
IS
  ln_days    NUMBER:=0;
		BEGIN
		     ln_days := NVL (pv_num_days, 0);
		     write_log_prc ('Number Of Days to Retain, Set to: '||ln_days||' Days');

							write_log_prc ('Delete xxd_cst_duty_ele_upld_stg_t Table Data Based on Purge Parameter:');
							write_log_prc ('-------------------------------------------------------------------------');

							BEGIN

												DELETE xxdo.xxd_cst_duty_ele_upld_stg_t
													WHERE TRUNC (creation_date) <= (TRUNC (sysdate) - ln_days);

							     write_log_prc (SQL%ROWCOUNT||' Rows Deleted from xxdo.xxd_cst_duty_ele_upld_stg_t Table');

							EXCEPTION
							     WHEN OTHERS
												THEN
												    write_log_prc ('Failed to Delete Records from xxdo.xxd_cst_duty_ele_upld_stg_t Table');
							END;
END purge_xxd_cst_duty_ele_upld_prc;


/**********************************************************************************
-- PROCEDURE purge_xxd_cst_duty_ele_cat_prc
-- PURPOSE: This Procedure Purge the Data from xxd_cst_duty_ele_cat_stg_t Table
***********************************************************************************/
PROCEDURE purge_xxd_cst_duty_ele_cat_prc (errbuf          OUT NOCOPY    VARCHAR2
																																									,retcode         OUT NOCOPY    VARCHAR2
																																									,pv_num_days                IN NUMBER)
IS
  ln_days    NUMBER:=0;
		BEGIN
		     ln_days := NVL (pv_num_days, 0);
		     write_log_prc ('Number Of Days to Retain, Set to: '||ln_days||' Days');

							write_log_prc ('Delete xxd_cst_duty_ele_cat_stg_t Table Data Based on Purge Parameter:');
							write_log_prc ('-------------------------------------------------------------------------');

							BEGIN

												DELETE xxdo.xxd_cst_duty_ele_cat_stg_t
													WHERE TRUNC (creation_date) <= (TRUNC (sysdate) - ln_days);

							     write_log_prc (SQL%ROWCOUNT||' Rows Deleted from xxdo.xxd_cst_duty_ele_cat_stg_t Table');

							EXCEPTION
							     WHEN OTHERS
												THEN
												    write_log_prc ('Failed to Delete Records from xxdo.xxd_cst_duty_ele_cat_stg_t Table');
							END;
END purge_xxd_cst_duty_ele_cat_prc;	

/**************************************************************************
-- PROCEDURE purge_xxdo_invval_duty_cost_prc
-- PURPOSE: This Procedure Purge xxdo_invval_duty_cost Table Data.
***************************************************************************/
PROCEDURE purge_xxdo_invval_duty_cost_prc (errbuf          OUT NOCOPY    VARCHAR2
																																										,retcode         OUT NOCOPY    VARCHAR2
																																										,pv_num_days                IN NUMBER)
IS
  ln_days    NUMBER:=0;
		ln_tot_cnt NUMBER:=0;
		ln_ins_cnt NUMBER:=0;
		ln_del_cnt NUMBER:=0;

		BEGIN
		     ln_days := NVL (pv_num_days, 0);
		     write_log_prc ('Number Of Days to Retain, Set to: '||ln_days||' Days');

							write_log_prc ('Delete xxdo_invval_duty_cost Table Data Based on Purge Parameter:');
							write_log_prc ('-----------------------------------------------------------------');

							BEGIN
							     ln_tot_cnt := 0;
												SELECT COUNT(1)
												  INTO ln_tot_cnt
														FROM xxdo.xxdo_invval_duty_cost;

														write_log_prc ('xxdo.xxdo_invval_duty_cost Total Count: '||ln_tot_cnt);

       EXCEPTION
							     WHEN OTHERS
												THEN
												    write_log_prc ('Failed to Retrive count of XXDO.XXDO_INVVAL_DUTY_COST');
							END;

							ln_ins_cnt := 0;
							ln_del_cnt := 0;

							BEGIN
												INSERT INTO xxdo.xxdo_invval_duty_cost_bkp (operating_unit,
																																																								country_of_origin,
																																																								primary_duty_flag,
																																																								oh_duty,
																																																								oh_nonduty,
																																																								additional_duty,
																																																								inventory_org,
																																																								inventory_item_id,
																																																								duty,
																																																								duty_start_date,
																																																								duty_end_date,
																																																								style_color,
																																																								last_update_date,
																																																								last_updated_by,
																																																								creation_date,
																																																								created_by,
																																																								freight,
																																																								freight_duty,
																																																								request_id)
																																																	SELECT operating_unit,
																																																								country_of_origin,
																																																								primary_duty_flag,
																																																								oh_duty,
																																																								oh_nonduty,
																																																								additional_duty,
																																																								inventory_org,
																																																								inventory_item_id,
																																																								duty,
																																																								duty_start_date,
																																																								duty_end_date,
																																																								style_color,
																																																								last_update_date,
																																																								last_updated_by,
																																																								creation_date,
																																																								created_by,
																																																								freight,
																																																								freight_duty,
																																																								gn_request_id
																																																			FROM xxdo.xxdo_invval_duty_cost
																																																		WHERE duty_end_date IS NOT NULL
																																																				AND TRUNC (duty_end_date) <= TRUNC (sysdate) - ln_days
																																																				AND TRUNC (creation_date) <= TRUNC (sysdate) - ln_days;

												write_log_prc (SQL%ROWCOUNT||' Rows Inserted Into XXDO.XXDO_INVVAL_DUTY_COST_BKP Table');							
												ln_ins_cnt := SQL%ROWCOUNT;

							EXCEPTION
							     WHEN OTHERS
												THEN
												    ln_ins_cnt := 0;
																write_log_prc ('Failed to Insert Records in Backup Table XXDO.XXDO_INVVAL_DUTY_COST_BKP');
							END;

							BEGIN

												DELETE xxdo.xxdo_invval_duty_cost
													WHERE duty_end_date IS NOT NULL
															AND TRUNC (duty_end_date) <= TRUNC (sysdate) - ln_days
															AND TRUNC (creation_date) <= TRUNC (sysdate) - ln_days;

												write_log_prc (SQL%ROWCOUNT||' Rows Deleted from XXDO.XXDO_INVVAL_DUTY_COST Table');
												ln_del_cnt := SQL%ROWCOUNT;

												IF ln_ins_cnt = ln_del_cnt
												THEN
																write_log_prc ('Deletion Successful-Commit');
																COMMIT;											
												ELSE
																write_log_prc ('Deletion Failed-Rollback');
																ROLLBACK;
												END IF;

       EXCEPTION
							     WHEN OTHERS
												THEN
												    write_log_prc ('Failed to Delete Records from XXDO.XXDO_INVVAL_DUTY_COST');
							END;

	 EXCEPTION
		     WHEN OTHERS
							THEN
							    write_log_prc ('Exception Occured in Purge Procedure-'||SQLERRM);
											retcode := gn_error;
END purge_xxdo_invval_duty_cost_prc;

	/***************************************************************************
	-- PROCEDURE main_prc
	-- PURPOSE: This Procedure is Concurrent Program.
	****************************************************************************/ 
	PROCEDURE main_prc (errbuf            OUT NOCOPY    VARCHAR2,
																				 retcode           OUT NOCOPY    VARCHAR2,
																					-- pv_duty_override             IN VARCHAR2,
                     pv_send_mail                 IN VARCHAR2)
 IS 
			CURSOR get_file_cur 
			IS
					SELECT filename
							-- FROM xxd_dir_list_tbl_syn
       FROM xxd_utl_file_upload_gt
						WHERE 1 = 1
						  AND UPPER (filename) like UPPER ('123060_DutyPlatform%')
			ORDER BY filename;

			CURSOR c_write_errors
			IS
			  --SELECT LISTAGG(distinct style_number,',' on overflow truncate with count) WITHIN GROUP (ORDER BY style_number) style_number, error_msg, COUNT(1) err_cnt
					SELECT error_msg, COUNT(1) err_cnt
					  FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t
						WHERE rec_status = 'E'
						  AND error_msg IS NOT NULL
						 -- AND group_id = p_group_id
						  AND request_id = gn_request_id        
			GROUP BY error_msg;

			lv_directory_path       VARCHAR2 (1000);
			lv_inb_directory_path   VARCHAR2 (1000);
			lv_arc_directory_path   VARCHAR2 (1000);
			lv_exc_directory_path   VARCHAR2 (1000);
			lv_directory            VARCHAR2 (1000);
			lv_file_name            VARCHAR2 (1000);
			lv_exc_file_name        VARCHAR2 (1000);
			lv_ret_message          VARCHAR2 (4000) := NULL;
			lv_ret_code             VARCHAR2 (30) := NULL;
			ln_file_exists      				NUMBER;
			lv_line             				VARCHAR2 (32767) := NULL;
			lv_all_file_names       VARCHAR2 (4000) := NULL;
			ln_rec_fail             NUMBER := 0;
			ln_rec_success          NUMBER;
			ln_rec_total            NUMBER;
			lv_mail_delimiter       VARCHAR2(1):='/';
			lv_result               VARCHAR2 (100);
			lv_result_msg           VARCHAR2 (4000);
			lv_message1             VARCHAR2 (32000);
			lv_message2             VARCHAR2 (32000);
			lv_message3             VARCHAR2 (32000);
			lv_sender               VARCHAR2 (100);
			lv_recipients           VARCHAR2 (4000);
			lv_ccrecipients         VARCHAR2 (4000);
			l_cnt                   NUMBER := 0;
			ln_req_id               NUMBER;
			lv_phase                VARCHAR2 (100);
			lv_status               VARCHAR2 (30);
			lv_dev_phase            VARCHAR2 (100);
			lv_dev_status           VARCHAR2 (100);
			lb_wait_req             BOOLEAN;
			lv_message              VARCHAR2 (1000);
			ln_ele_rec_total        NUMBER;
			-- lv_duty_override        VARCHAR2(1);

			BEGIN 
			     write_log_prc ('Main Process Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
			     lv_exc_file_name := NULL;
								lv_file_name := NULL;
								-- Derive the directory Path

								BEGIN
													lv_inb_directory_path := NULL;
													lv_directory := 'XXD_CST_DUTY_ELE_INB_DIR';

													SELECT directory_path
															INTO lv_inb_directory_path
															FROM dba_directories
														WHERE 1 = 1
														  AND directory_name LIKE 'XXD_CST_DUTY_ELE_INB_DIR';

								EXCEPTION
												 WHEN OTHERS
											 	THEN
																lv_inb_directory_path := NULL;
																write_log_prc (' Exception Occurred while retriving the Inbound Directory');
								END;

								BEGIN
													lv_arc_directory_path := NULL;

													SELECT directory_path
															INTO lv_arc_directory_path
															FROM dba_directories
														WHERE 1 = 1
														  AND directory_name LIKE 'XXD_CST_DUTY_ELE_ARC_DIR';

								EXCEPTION
													WHEN OTHERS
													THEN
																lv_arc_directory_path := NULL;
																write_log_prc (' Exception Occurred while retriving the Archive Directory');
								END;

								BEGIN
													lv_exc_directory_path := NULL;

													SELECT directory_path
															INTO lv_exc_directory_path
															FROM dba_directories
														WHERE 1 = 1
														  AND directory_name LIKE 'XXD_CST_DUTY_ELE_EXC_DIR';

								EXCEPTION
													WHEN OTHERS
													THEN
																lv_exc_directory_path := NULL;
																write_log_prc (' Exception Occurred while retriving the Exception Directory');
								END;

								-- Now Get the file names
								get_file_names (lv_inb_directory_path);

								FOR data IN get_file_cur
								LOOP
												ln_file_exists := 0;
												lv_file_name := NULL;
												lv_file_name := data.filename;

												write_log_prc (' File is availale - '||lv_file_name);

												-- Check the file name exists in the table if exists then SKIP

												BEGIN

																	SELECT COUNT(1)
																			INTO ln_file_exists
																			FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t
																		WHERE 1 = 1
																				AND UPPER(filename) =  UPPER(lv_file_name);

												EXCEPTION
																	WHEN OTHERS
																	THEN
															     	ln_file_exists := 0;    
												END;

												IF ln_file_exists = 0
												THEN

																load_file_into_tbl_prc (pv_table                => 'XXD_CST_DUTY_ELE_INB_STG_TR_T',
																																								pv_dir                  => 'XXD_CST_DUTY_ELE_INB_DIR',
																																								pv_filename             => lv_file_name,
																																								pv_ignore_headerlines   => 1,
																																								pv_delimiter            => '|',
																																								pv_optional_enclosed    => '"',
																																								pv_num_of_columns       => 46);       -- Change the number of columns

																BEGIN

																					UPDATE xxdo.xxd_cst_duty_ele_inb_stg_tr_t
																								SET filename = lv_file_name, 
																												request_id = gn_request_id,
																												creation_date = sysdate, 
																												last_update_date = SYSDATE,
																												created_by = gn_user_id, 
																												last_updated_by = gn_user_id,
																												active_flag = 'Y',
																												rec_status = 'N'
																						WHERE 1 = 1
																								AND filename IS NULL 
																								AND request_id IS NULL;

																				write_log_prc (SQL%ROWCOUNT||' Records updated with Filename, Request ID and WHO Columns');

																EXCEPTION
																					WHEN OTHERS
																					THEN
																									write_log_prc ('Error Occured while Updating the Filename, Request ID and WHO Columns-'||SQLERRM);
																END;

																COMMIT;

															 -- lv_duty_override := NVL (pv_duty_override,'N');

																-- validate_prc (lv_file_name, lv_duty_override);
																validate_prc (lv_file_name);

																-- insert_into_custom_table_prc (lv_duty_override);

																BEGIN

																					write_log_prc ('Move files Process Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
																					ln_req_id := fnd_request.submit_request (application   => 'XXDO',
																																																														program       => 'XXDO_CP_MV_RM_FILE',
																																																														argument1     => 'MOVE',                                         -- MODE : COPY, MOVE, RENAME, REMOVE
																																																														argument2     => 2,
																																																														argument3     => lv_inb_directory_path||'/'||lv_file_name,                  -- Source File Directory
																																																														argument4     => lv_arc_directory_path||'/'||SYSDATE||'_'||lv_file_name,    -- Destination File Directory
																																																														start_time    => SYSDATE,
																																																														sub_request   => FALSE);
																					COMMIT;

																					IF ln_req_id = 0
																					THEN
																									retcode := gn_warning;
																									write_log_prc (' Unable to submit move files concurrent program ');
																					ELSE
																									write_log_prc ('Move Files concurrent request submitted successfully.');
																									lb_wait_req :=  fnd_concurrent.wait_for_request (request_id   => ln_req_id,
																																																																										interval     => 5,
																																																																										phase        => lv_phase,
																																																																										status       => lv_status,
																																																																										dev_phase    => lv_dev_phase,
																																																																										dev_status   => lv_dev_status,
																																																																										message      => lv_message);

																									IF lv_dev_phase = 'COMPLETE' AND lv_dev_status = 'NORMAL'
																									THEN
																														write_log_prc ('Move Files concurrent request with the request id '|| ln_req_id|| ' completed with NORMAL status.');
																									ELSE
																													retcode := gn_warning;
																													write_log_prc ( 'Move Files concurrent request with the request id '|| ln_req_id	|| ' did not complete with NORMAL status.');
																									END IF;          -- End of if to check if the status is normal and phase is complete
																					END IF;              -- End of if to check if request ID is 0.

																					COMMIT;
																					write_log_prc ('Move Files Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

																EXCEPTION
																					WHEN OTHERS
																					THEN
																									retcode := gn_error;
																									write_log_prc ('Error in Move Files -'||SQLERRM);
																END;

														ELSIF ln_file_exists > 0 THEN
																			--l_cnt := l_cnt + 1;
																			write_log_prc ('**************************************************************************************************');
																			write_log_prc ('Data with this File name - '||lv_file_name|| ' - is already loaded. Please change the file data.  '); 
																			write_log_prc ('**************************************************************************************************');
																			retcode := gn_warning;

																			BEGIN

																								write_log_prc ('Move files Process Begins...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));
																												ln_req_id := fnd_request.submit_request (application   => 'XXDO',
																																																																					program       => 'XXDO_CP_MV_RM_FILE',
																																																																					argument1     => 'MOVE',                                         -- MODE : COPY, MOVE, RENAME, REMOVE
																																																																					argument2     => 2,
																																																																					argument3     => lv_inb_directory_path||'/'||lv_file_name,                  -- Source File Directory
																																																																					argument4     => lv_arc_directory_path||'/'||SYSDATE||'_'||lv_file_name,    -- Destination File Directory
																																																																					start_time    => SYSDATE,
																																																																					sub_request   => FALSE);
																								COMMIT;

																								IF ln_req_id = 0
																								THEN
																												retcode := gn_warning;
																												write_log_prc (' Unable to submit move files concurrent program ');
																								ELSE
																												write_log_prc ('Move Files concurrent request submitted successfully.');
																												lb_wait_req :=  fnd_concurrent.wait_for_request (request_id   => ln_req_id,
																																																																													interval     => 5,
																																																																													phase        => lv_phase,
																																																																													status       => lv_status,
																																																																													dev_phase    => lv_dev_phase,
																																																																													dev_status   => lv_dev_status,
																																																																													message      => lv_message);

																												IF lv_dev_phase = 'COMPLETE' AND lv_dev_status = 'NORMAL'
																												THEN
																																	write_log_prc ('Move Files concurrent request with the request id '|| ln_req_id|| ' completed with NORMAL status.');
																												ELSE
																																retcode := gn_warning;
																																write_log_prc ( 'Move Files concurrent request with the request id '|| ln_req_id	|| ' did not complete with NORMAL status.');
																												END IF;          -- End of if to check if the status is normal and phase is complete
																								END IF;              -- End of if to check if request ID is 0.

																								COMMIT;
																								write_log_prc ('Move Files Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));

																			EXCEPTION
																								WHEN OTHERS 
																								THEN
																												write_log_prc ('File already exists, Error Occured while Copying/Removing file from Inbound directory, Check File Privileges: '||SQLERRM);
																												retcode := gn_warning;
																			END;

														END IF; 

								EXIT WHEN get_file_cur%NOTFOUND;										
								END LOOP;

								COMMIT;

						  insert_into_custom_table_prc;

								BEGIN
														SELECT COUNT(1)
																INTO ln_rec_fail
																FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t
															WHERE 1 = 1
																	AND rec_status = 'E'
																	AND error_msg IS NOT NULL
																	AND request_id = gn_request_id;
																	-- AND UPPER(filename) =  UPPER(lv_file_name);

								EXCEPTION
													WHEN OTHERS
													THEN
																	ln_rec_fail := 0;
																	write_log_prc ('Exception Occurred while retriving the Error Count');
								END;

								-- IF ln_file_exists IS NULL
								-- THEN
												-- write_log_prc ('There is nothing to Process...No File Exists.');
												-- retcode := gn_warning;
								-- ELSE

								SELECT COUNT(1)
										INTO ln_rec_total
										FROM xxdo.xxd_cst_duty_ele_inb_stg_tr_t
									WHERE request_id = gn_request_id;

								ln_rec_success := ln_rec_total - ln_rec_fail;

								apps.fnd_file.put_line(apps.fnd_file.output,'                                                                      Deckers TRO Inbound Duty Elements Upload');											
								apps.fnd_file.put_line(apps.fnd_file.output,'');
								apps.fnd_file.put_line(apps.fnd_file.output,'Date:'||TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
								apps.fnd_file.put_line(apps.fnd_file.output,'');
								apps.fnd_file.put_line(apps.fnd_file.output,'');
								apps.fnd_file.put_line(apps.fnd_file.output,'');
								apps.fnd_file.put_line(apps.fnd_file.output,'************************************************************************');
								apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Considered into Inbound Staging Table - '||ln_rec_total);
								apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Errored                               - '||ln_rec_fail);
								apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Successful                            - '||ln_rec_success);
								apps.fnd_file.put_line(apps.fnd_file.output,'************************************************************************');
								apps.fnd_file.put_line(apps.fnd_file.output,'');
								apps.fnd_file.put_line(apps.fnd_file.output,'');
								apps.fnd_file.put_line(apps.fnd_file.output,'');

								BEGIN
													SELECT COUNT(1)
															INTO ln_ele_rec_total
															FROM xxdo.xxd_cst_duty_ele_upld_stg_t
														WHERE 1 = 1
																AND rec_status = 'N'
																AND error_msg IS NULL
																AND active_flag = 'Y'
																AND request_id = gn_request_id;

													apps.fnd_file.put_line(apps.fnd_file.output,'*************************************************************************************************');
													apps.fnd_file.put_line(apps.fnd_file.output,' Number of Rows Successfully Inserted in Process Staging Table        - '||ln_ele_rec_total);
													apps.fnd_file.put_line(apps.fnd_file.output,'*************************************************************************************************');


								EXCEPTION
													WHEN Others
													THEN
																	write_log_prc ('Exception occurred while retriving the count from xxdo.xxd_cst_duty_ele_inb_stg_tr_t');
																	ln_ele_rec_total := 0;
								END;

								-- END IF;

								IF ln_rec_fail > 0
								THEN
											generate_exception_report_prc (lv_exc_file_name);
											lv_exc_file_name := lv_exc_directory_path||lv_mail_delimiter||lv_exc_file_name;
											write_log_prc (lv_exc_file_name);

											IF NVL (pv_send_mail,'N') = 'Y'
											THEN

															lv_message2 :=          '************************************************************************'||
																														CHR(10)||' Number of Rows Considered into Inbound Staging Table - '||ln_rec_total||'.'||
																														CHR(10)||' Number of Rows Errored - '||ln_rec_fail||'.'||
																														CHR(10)||' Number of Rows Successful - '||ln_rec_success||'.'||
																														CHR(10)||'************************************************************************'||
																														CHR(10)||
																														CHR(10)||'Distinct Error Messages :'||
																														CHR(10)||'========================='||
																														CHR(10)||'Count'||CHR(9)||'Error Message'||
																														CHR(10)||'-----------------------------------------------------------------';

															FOR i in c_write_errors
															LOOP
																			--lv_message3 := lv_message3||CHR(10)||i.err_cnt||CHR(9)||i.error_msg;
																			lv_message3 := CASE WHEN lv_message3 IS NOT NULL
																																		THEN
																																						lv_message3||'.'||CHR(10)||i.err_cnt||'.'||CHR(9)||i.error_msg
																																		ELSE 
																																						i.err_cnt||'.'||CHR(9)||i.error_msg
																																		END;
															END LOOP;

															lv_message3 := SUBSTR (lv_message3,1,30000);

															lv_message1 := 'Hello Team,'||CHR(10)||CHR(10)||'Please Find the Attached Deckers TRO Inbound Duty Elements Exception Report. '||
															CHR(10)||CHR(10)||lv_message2||CHR(10)||lv_message3||CHR(10)||CHR(10)||'Regards,'||CHR(10)||'SYSADMIN.'|| CHR (10)|| CHR (10)|| 'Note: This is auto generated mail, please donot reply.';

															SELECT LISTAGG (ffvl.description, ';') WITHIN GROUP (ORDER BY ffvl.description)
																	INTO lv_recipients
																	FROM apps.fnd_flex_value_sets  fvs,
																						apps.fnd_flex_values_vl   ffvl
																WHERE fvs.flex_value_set_id = ffvl.flex_value_set_id
																		AND fvs.flex_value_set_name = 'XXD_TRO_EMAIL_TO_PD_VS'
																		AND NVL (TRUNC (ffvl.start_date_active),TRUNC (SYSDATE)) <=TRUNC (SYSDATE)
																		AND NVL (TRUNC (ffvl.end_date_active),TRUNC (SYSDATE)) >=TRUNC (SYSDATE)
																		AND ffvl.enabled_flag = 'Y';

															xxdo_mail_pkg.send_mail (pv_sender       => 'erp@deckers.com',
																																								pv_recipients   => lv_recipients,
																																								pv_ccrecipients => lv_ccrecipients,
																																								pv_subject      => 'Deckers TRO Inbound Duty Elements Exception Report',
																																								pv_message      => lv_message1,
																																								pv_attachments  => lv_exc_file_name,
																																								xv_result       => lv_result,
																																								xv_result_msg   => lv_result_msg);

															write_log_prc (lv_result);
															write_log_prc (lv_result_msg);

											END IF;
								END IF;

								write_log_prc ('Main Process Ends...'||TO_CHAR (SYSDATE, 'dd-mon-yyyy hh:mi:ss'));	

			EXCEPTION
			     WHEN OTHERS
			     THEN
												errbuf := SQLERRM;
												retcode := gn_error;
												write_log_prc ('Error Occured in Procedure main_prc: '||SQLERRM);							
 END main_prc;

END xxd_cst_duty_ele_inb_tr_pkg;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
