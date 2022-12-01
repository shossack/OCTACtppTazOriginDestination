#-------------------------------------------------------------------------------
# Name:        ScriptToolCTPP.py
# Purpose:     This runs from a Geoprocessing server that calls the
#              CTPP API. It submits an ORIGIN TAZ and pulls a list
#              of DESTINATION TAZ'S with the number of workers traveling
#              from ORIGIN TAZ to DESTINATION TAZ
#
#              This script was submitted to the CTPP API Hackathon 11/30/2022
#
# Author:      steve hossack, mark jackman
#
# Created:     29/11/2022
# Copyright:   (c) shossack 2022
#
#-------------------------------------------------------------------------------


import requests
import os
import arcpy

def ParseTAZ(s):

    # parse the TAZ id from the "name"
    outStr = ""
    try:
        outStr = s.split(",")[0].split("TAZ ")[1]
    except:
        outStr = ""

    return outStr

def ParseDestinationFromGeoID(s):

    # get the destination GEOID
    outStr = ""
    try:
        outStr = s[-13:]
    except:
        outStr = ""

    return outStr

def ScriptTool(param0):

    # input parameter is the feature layer of CTPP TAZ in OC
    inputTAZLayer = param0

    # use an SDE file to get access to Enterprise Geodatabase
    sdeFile = r'SDEFILE.sde'
    selected_taz_list_table = 'Scratch.DBO.CTPP_SELECTED_OD_TAZ'
    taz_list_full = sdeFile + r'\\' + selected_taz_list_table


    # Get API key (sorry it's hardcoded)
    key = '<< use your own >>'
    header = {"x-api-key": key}

    # Set base URL for API
    server = "http://ctpp.macrosysrt.com/api"

    # from the input layer, get the TAZ id of the selected ORIGIN TAZ
    idList = [row[0] for row in arcpy.da.SearchCursor(inputTAZLayer,["TAZCE10"])]

    # get the ID of the selected ORIGIN taz
    # if no TAZ's are selected, just use the sample TAZ
    if len(idList) < 1:
        input_taz = "32588000"
    else:
        input_taz = idList[0]

    try:

        # construct endpoint of URL with ORIGIN TAZ id
        # it will query the "B3202203" table of workers traveling from TAZ->TAZ
        endpoint = "/data/2016?get=B302203_e1&for=traffic%20analysis%20zone:" + input_taz + "&in=state:06&in=county:059&d-for=traffic%20analysis%20zone:*&in=state:06&in=county:059"

        # call the API server and get the response
        response = requests.get(server+endpoint, headers=header)
        #print(response.text)
        print("&"*200)

        # loop through response JSON and get the DESTINATION TAZ id's and
        # store them in "outList"
        outList = []
        output = response.json()
        for rec in output['data']:
            o_name = rec["origin_name"]
            d_name = rec["destination_name"]
            workers = rec["b302203_e1"]  # this has the number of total workers estimated from TAZ->TAZ
            d_geoid = ParseDestinationFromGeoID(rec["geoid"])

            print(ParseTAZ(d_name) + "::" + workers + ":: geoid: " + d_geoid)
            arcpy.AddMessage(ParseTAZ(d_name) + "::" + workers + ":: geoid: " + d_geoid)
            outList.append((ParseTAZ(d_name), workers, d_geoid))
    except:
        arcpy.AddError("Error contacting " + server)
        return

    try:

        # there's a stand alone table in our Enterprise Geodatabase to store the origin/destination taz id's
        # this allows for a layer to be displayed of origins and destinations
        # first clear out the table
        egdb_conn = arcpy.ArcSDESQLExecute(sdeFile)
        egdb_return = egdb_conn.execute("TRUNCATE TABLE " + selected_taz_list_table)

        # write the single ORIGIN taz id to table
        with arcpy.da.InsertCursor(taz_list_full,["TAZ_ID","ODFlag"]) as iocursor:
            iocursor.insertRow([input_taz,0])

        # write the DESTINATION taz id's to table
        with arcpy.da.InsertCursor(taz_list_full,["TAZ_ID","WORKERS","GEOID", "ODFlag"]) as idcursor:
            for rec in outList:
                idcursor.insertRow([rec[0], rec[1], rec[2], 1])

        arcpy.AddMessage("Done!")

    except:
        arcpy.AddError("Error writing to database at: " + sdeFile)
        return



# This is used to execute code if the file was run but not imported
if __name__ == '__main__':
    # Tool parameter accessed with GetParameter or GetParameterAsText
    param0 = arcpy.GetParameterAsText(0)

    ScriptTool(param0)

