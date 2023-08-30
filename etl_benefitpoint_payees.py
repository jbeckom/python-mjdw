import os
import mjdb
import bpws
import config
import common as cmn
import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine

LOGDIR = 'etl_benefitpoint'
SCHEMA = 'benefitpoint'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
WSTSFMT = '%Y-%m-%dT%H:%M:%S.%f%z'

lf = cmn.log_filer(LOGDIR, 'payees')

def payee_ids(lastMod):
    if (dt.datetime.now() - lastMod).days > 30:
        fpResp = bpws.find_payees()
        fpSoup = bs(fpResp.content,'xml')
        if fpResp.ok==False:
            raise ValueError(f"status_code: {fpResp.status_code}, faultCode: {fpSoup.find('faultcode').text}, faultString: {fpSoup.find('faultstring').text}")
        else:
            return [s.find('payeeID').text for s in fpSoup.find_all('summaries')]
    else:
        fcResp = bpws.find_changes(sinceLastModifiedOn=lastMod, typesToInclude='Payee')
        fcSoup = bs(fpResp.content,'xml')
        if fpResp.ok==False:
            raise ValueError(f"status_code: {fcResp.status_code}, faultCode: {fcSoup.find('faultcode').text}, faultString: {fcSoup.find('faultstring').text}")
        else:
            return [m.find('entityID').text for m in fcSoup.find_all('modifications')]

def payee_row(payeeID, soup):
    row = {'payee_id':int(payeeID)}
    for b in ('house_account','include_in_book_of_business','over_payement_payee','internal_payee'):
        tag = cmn.bp_col_to_tag(b)
        row[b] = cmn.bp_parse_bool(soup.find(tag).text) if soup.find(tag) else None
    for f in ('revenue_goal','commission_goal','renewal_revenue_goal','renewal_commission_goal','replacement_revenue_goal','replacement_commission_goal'):
        tag = cmn.bp_col_to_tag(f)
        row[f] = float(soup.find(tag).text) if soup.find(tag) else None
    for s in ('payee_type','payee_code','tax_payer_id_number','notes'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    for t in ('last_modified_on','created_on'):
        tag = cmn.bp_col_to_tag(t)
        row[t] = dt.datetime.strptime(soup.find(tag).text,WSTSFMT) if soup.find(tag) else None
    return row

def individual_payee_row(payeeID,soup):
    row = {'payee_id':int(payeeID)}
    for i in ('agent_account_id','user_id'):
        tag = cmn.bp_col_to_tag(i).replace('Id','ID')
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for s in ('tax_status','company_name','department_code','employee_code'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def company_payee_row(payeeID, soup):
    row = {
        'payee_id':int(payeeID),
        'agency_account_id':int(soup.find('agencyAccountID').text) if soup.find('agencyAccountID') else None,
        'company_1099':cmn.bp_parse_bool(soup.find('company1099').text) if soup.find('company1099') else None
    }
    for s in ('company_name','vendor_number','website'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def payee_details(payeeType, payeeID, payeeSoup):
    if payeeType=='Individual_Payee':
        payeeDetail = individual_payee_row(payeeID, payeeSoup)
    elif payeeType=='Company_Payee':
        payeeDetail = company_payee_row(payeeID, payeeSoup)
    else:
        raise ValueError(f"invalid payeeType: {payeeType}")
    contact = cmn.bp_contact_row('PAYEE', payeeID, payeeSoup.find('contactID').text, payeeSoup.find('contact'))
    address = cmn.bp_address_row('PAYEE', payeeType.upper(), payeeID, payeeSoup.find('address')) if len(payeeSoup.find('address').find_all()) > 0 else None
    phones = [cmn.bp_phone_row('PAYEE', payeeType.upper(), payeeID, ps) for ps in payeeSoup.find_all('phones') if not ps.has_attr('xsi:nil')]
    return payeeDetail, contact, address, phones

def payee_team_member_row(payeeID, teamMemberID, soup):
    return {
        'payee_id':int(payeeID),
        'team_member_id':int(teamMemberID),
        'commission':float(soup.find('commission').text) if soup.find('commission') else None
    }

def main():
    payeeRows = []
    individualPayeeRows = []
    companyPayeeRows = []
    contactRows = []
    addressRows = []
    phoneRows = []
    licenseRows = []
    carrierAppointmentRows = []
    payeeTeamMemberRows = []
    try:
        lastMod = mjdb.bp_last_modified('payee') if mjdb.bp_last_modified('payee') else dt.datetime(1900,1,1,0,0)
    except Exception as e:
        lf.error(f"unable to retrieve Last Modified date:\n{e}")
    else:
        try:
            payeeIDs = payee_ids(lastMod)
        except Exception as e:
            lf.error(f"unable to retrieve payees:\n{e}")
        else:
            for pid in payeeIDs:
                try:
                    gpResp = bpws.get_payee(pid)
                    gpSoup = bs(gpResp.content,'xml')
                    if gpResp.ok==False:
                        raise ValueError(f"status_code: {gpResp.status_code}, faultCode: {gpSoup.find('faultcode').text}, faultString: {gpSoup.find('faultstring').text}")
                    else:
                        try:
                            payeeRows.append(payee_row(pid,gpSoup))
                        except Exception as e:
                            lf.error(f"unable to parse payee_row for {pid}:\n{e}")
                        else:
                            try:
                                payeeType = gpSoup.find('payeeType').text
                                payeeDetail = payee_details(payeeType, pid, (gpSoup.find('individualPayee') if payeeType=='Individual_Payee' else gpSoup.find('companyPayee')))
                                individualPayeeRows.append(payeeDetail[0])
                                contactRows.append(payeeDetail[1])
                                addressRows.append(payeeDetail[2]) if payeeDetail[2] else None
                                [phoneRows.append(p) for p in payeeDetail[3]]
                            except Exception as e:
                                lf.error(f"unable to parse payee_details for {pid}:\n{e}")
                            try:
                                [licenseRows.append(cmn.bp_license_row('PAYEE',pid,l.find('licenseID').text,l)) for l in gpSoup.find_all('licenses')]
                            except Exception as e:
                                lf.error(f"unable to parse Licenses for {pid}:\n{e}")
                            try:
                                [carrierAppointmentRows.append(cmn.bp_carrier_appointment_row('PAYEE', pid, ca.find('carrierAppointmentID').text, ca)) for ca in gpSoup.find_all('carrierAppointments')]
                            except Exception as e:
                                lf.error(f"unable to parse CarrierAppointments for {pid}:\n{e}")
                except Exception as e:
                    lf.error(f"unable to parse getPayee for {pid}\n{e}")
        stages = {
            'payee':payeeRows if payeeRows else None,
            'individual_payee':individualPayeeRows if individualPayeeRows else None,
            'company_payee':companyPayeeRows if companyPayeeRows else None,
            'contact':contactRows if contactRows else None,
            'address':addressRows if addressRows else None,
            'phone':phoneRows if phoneRows else None,
            'license':licenseRows if licenseRows else None,
            'carrier_appointment':carrierAppointmentRows if carrierAppointmentRows else None,
            'payee_team_member':payeeTeamMemberRows if payeeTeamMemberRows else None
        }
        for s in stages:
            if stages[s]:
                try:
                    rcs = pd.DataFrame(stages[s]).drop_duplicates().to_sql(f'stg_{s}',ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
                except Exception as e:
                    lf.error(f"unable to stage records for {s}:\n{e}")
                else:
                    lf.info(f"{rcs} record(s) staged for {s}")
                    if rcs > 0:
                        try:
                            rcu = mjdb.upsert_stage(SCHEMA, s, 'upsert')
                        except Exception as e:
                            lf.error(f"unable to upsert from stage to {s}:\n{e}")
                        else:
                            lf.info(f"{rcu} record(s) affected for {s}")
                    else:
                        lf.info(f"no records to stage for {s}")
                finally:
                    mjdb.drop_table(SCHEMA, f'stg_{s}')

if __name__ == '__main__':
    main()
