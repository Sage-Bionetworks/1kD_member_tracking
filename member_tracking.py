"""
Name: data_request_tracking.py
Description: a script to generate data_request_tracking table, data request change logs table,
             data structure tree and IDU wiki page for 1kD project
Contributors: Dan Lu
"""
import json
import logging
import os
import pdb
import tempfile
import typing
# import modules
from datetime import datetime
from doctest import testmod

import numpy as np
import pandas as pd
import synapseclient
from synapseclient import File, Table
from synapseclient.core.exceptions import (SynapseAuthenticationError,
                                           SynapseNoCredentialsError)
from synapseutils import walk

# adapted from challengeutils https://github.com/Sage-Bionetworks/challengeutils/pull/121/files to manage Synapse connection
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class Synapse:
    """Define Synapse class"""

    _synapse_client = None

    @classmethod
    def client(cls, syn_user=None, syn_pass=None, *args, **kwargs):
        """Gets a logged in instance of the synapseclient.
        Args:
            syn_user: Synapse username
            syn_pass: Synpase password
        Returns:
            logged in synapse client
        """
        if not cls._synapse_client:
            LOGGER.debug("Getting a new Synapse client.")
            cls._synapse_client = synapseclient.Synapse(*args, **kwargs)
            try:
                if os.getenv("SCHEDULED_JOB_SECRETS") is not None:
                    secrets = json.loads(os.getenv("SCHEDULED_JOB_SECRETS"))
                    cls._synapse_client.login(
                        silent=True, authToken=secrets["SYNAPSE_AUTH_TOKEN"]
                    )
                else:
                    cls._synapse_client.login(
                        authToken=os.getenv("SYNAPSE_AUTH_TOKEN"), silent=True
                    )
            except SynapseAuthenticationError:
                cls._synapse_client.login(syn_user, syn_pass, silent=True)

        LOGGER.debug("Already have a Synapse client, returning it.")
        return cls._synapse_client

    @classmethod
    def reset(cls):
        """Change synapse connection"""
        cls._synapse_client = None


def print_green(t: str, st: str = None):
    """
    Function to print out message in green

    :param t (str): a string
    :param st (str, optional): the latter string. Defaults to None. Use this when you want to print out the latter string.
    """
    if st is None:
        print("\033[92m {}\033[00m".format(t))
    else:
        print("{}: \033[92m {}\033[00m".format(t, st))


def get_user_profile(team_id: str, return_profile: bool = True) -> list:
    """
    Function to get user profile and/or only user id

    :param team_id (str): 1kD team id
    :param return_profile (bool, optional): whether to return user profile. Defaults to True.

    :returns:  list: submitter_id, first_name, last_name, user_name, team_name or only user_id(s)
    """
    syn = Synapse().client()
    members = list(syn.getTeamMembers(syn.getTeam(team_id)))
    if return_profile:
        user_profile = pd.concat(
            [
                pd.DataFrame(
                    {**x["member"], **{"team_id": x["teamId"]}},
                    index=[0],
                )
                for x in members
            ]
        )
        user_profile["team_name"] = syn.getTeam(team_id)["name"]
        user_profile.drop(columns=["isIndividual", "team_id"], inplace=True)
        return user_profile.rename(
            columns={
                "ownerId": "submitter_id",
                "firstName": "first_name",
                "lastName": "last_name",
                "userName": "user_name",
            }
        )
    else:
        user_id = [member["member"]["ownerId"] for member in members]
        return user_id


def get_team_member() -> pd.DataFrame:
    """
    Function to pull team members and re-categorize team members if they are in ACT or admin team

    :returns:  pd.DataFrame: submitter_id, first_name, last_name, user_name, team_name
    """
    # get a list of team members
    team_ids = [
        "3436722",  # 1kD_Connectome
        "3436721",  # 1kD_InfantNaturalStatistics
        "3436720",  # 1kD_BRAINRISE
        "3436718",  # 1kD_KHULA
        "3436509",  # 1kD_MicrobiomeBrainDevelopment
        "3436717",  # 1kD_M4EFaD_LABS
        "3436716",  # 1kD_Assembloids
        "3436713",  # 1kD_DyadicSociometrics_NTU
        "3466183",  # 1kD_DyadicSociometrics_Cambridge
        "3436714",  # 1kD_First1000Daysdatabase
        "3458847",  # 1kD_M4EFaD_BMT team
        "3464137",  # 1kD_Stanford_Yeung team
        "3460645",  # 1kD_M4EFaD_Auckland
    ]
    members = pd.concat([get_user_profile(team_id) for team_id in team_ids])
    # update team name for DCC members
    admin = get_user_profile("3433360")
    act = get_user_profile("464532")
    members.loc[
        members["submitter_id"].isin(admin["submitter_id"].values), "team_name"
    ] = "1kD admins"
    members.loc[
        members["submitter_id"].isin(act["submitter_id"].values), "team_name"
    ] = "ACT"
    # collapse team_name for members that are in multiple teams
    members = members.groupby(["submitter_id","first_name", "last_name", "user_name"],dropna=False)["team_name"].apply(lambda x: ",".join(x.unique())).reset_index()
    members.drop_duplicates(inplace=True)
    return members.reset_index(drop=True)

def load_members_table(synapse_id: str)-> pd.DataFrame:
    syn = Synapse().client()
    member_table = syn.tableQuery(f"select * from {synapse_id}")
    member_table = member_table.asDataFrame()
    return member_table
     
def update_table(table_name: str, df: pd.DataFrame):
    """
    Function to update table with table name

    :param table_name (str): a table name
    :param df (pd.DataFrame): the data frame to be saved
    """
    syn = Synapse().client()
    tables = {
        "1kD Team Members": "syn35048407"
    }
    results = syn.tableQuery(f"select * from {tables[table_name]}")
    delete_out = syn.delete(results)
    table_out = syn.store(Table(tables[table_name], df), forceVersion = True)
    print_green(f"Done updating {table_name} table")

def membership_report(out_dir):
    """
    Function to update folder tree file

    :param out_dir (str): the directory saves the temp folders created in get_folder_tree()
    """
    syn = Synapse().client()
    today = datetime.today()
    with open(f"1kD_membership_report_{today.year}_{today.month}.csv", "w") as file:
        table_out = syn.store(
            File(
                f"1kD_membership_report_{today.year}_{today.month}.csv",
                parent="syn35023796",
            )
        )
        os.remove(f"1kD_membership_report_{today.year}_{today.month}.csv")

def main():
    members = get_team_member()
    member_table = load_members_table('syn35048407')
    member_table.drop(columns = ["first_name", "last_name"],inplace=True)
    #pdb.set_trace()
    # merge new members table with existing table 
    merged = members.merge(member_table, on = ["submitter_id", "user_name", "team_name"], how = 'outer', indicator = True)
    merged =  merged.loc[merged['_merge'] !='both',].reset_index(drop=True)
    merged.rename(columns = {'_merge':'Note'}, inplace=True)
    merged["Note"] = np.where( merged["Note"] == "left_only", 'added', 'removed')
    # save report to sage admin folder
    out_dir = tempfile.mkdtemp(dir=os.getcwd())
    membership_report(out_dir)
    #update member_table
    #update_table('1kD Team Members', members)

if __name__ == "__main__":
    main()
