from abc import ABC, abstractmethod
import mysql.connector
import boto3
import time
import json 
import sys
import os

class Workspace:
    """Workspace"""
    def __init__(self, _id, _name, _state, _mode, _type):
        self.id = _id
        self.name = _name 
        self.state = _state
        self.mode = _mode
        self.type = _type


class AWS(ABC):
    @abstractmethod
    def connect(self):
        pass


class RDS(AWS):
    """RDS Proxy"""
    def __init__(self):
        self.endpoint = os.getenv("RDS_ENDPOINT")
        self.port = os.getenv("RDS_PORT")
        self.region = os.getenv("RDS_REGION")
        self.database = os.getenv("RDS_DATABASE")
        self.user = os.getenv("RDS_USER")
        self.bundle = os.getenv("RDS_BUNDLE")
        self.token = None
        self.connection = None


    def connect(self):
        try:
            _rds = boto3.client("rds")
            self.token = _rds.generate_db_auth_token(
                            DBHostname=self.endpoint, 
                            Port=self.port, 
                            DBUsername=self.user, 
                            Region=self.region
                        )           
            
            self.connection = mysql.connector.connect(
                                host=self.endpoint, 
                                user=self.user, 
                                passwd=self.token, 
                                port=self.port, 
                                database=self.database, 
                                ssl_ca=self.bundle
                            )
        except Exception as e:
            print(f"Could not connect to DB: {e}")
            raise e


    def get_actions(self):
        try:
            _cursor = self.connection.cursor()
            _query = "SELECT id, name FROM actions"
            _cursor.execute(_query)

            _actions = {}
            for _action_id, _action_name in _cursor:
                _actions[_action_id] = _action_name
            return _actions

        except Exception as e:
            print(f"Could not get actions from DB: {e}")
            raise e


class WKS(AWS):
    """Workspaces Proxy"""
    def __init__(self):
        self.connection = None


    def connect(self):
        try:
            _wks = boto3.client("workspaces")
            self.connection = _wks
        except Exception as e:
            print(f"Could not connect to AWS: {e}")
            raise e


    def get_directory(self):
        try:        
            _directory = None
            _dirs_resp = self.connection.describe_workspace_directories()
            if len(_dirs_resp["Directories"]) > 0:
                _directory = _dirs_resp["Directories"][0]
                return _directory

        except Exception as e:
            print(f"Could not get workspaces from AWS: {e}")
            raise e


    def get_workspaces(self):
        try:        
            _workspaces = []
            _works_resp = self.connection.describe_workspaces()
            if len(_works_resp["Workspaces"]) > 0:
                for _workspace in _works_resp["Workspaces"]:
                    _this = Workspace(
                        _workspace["WorkspaceId"],
                        _workspace["UserName"],
                        _workspace["State"],
                        _workspace["WorkspaceProperties"]["RunningMode"],
                        _workspace["WorkspaceProperties"]["ComputeTypeName"],
                    )
                    _workspaces.append(_this)
            return _workspaces

        except Exception as e:
            print(f"Could not get workspaces from AWS: {e}")
            raise e


    def start_workspace(self, workspace):
        try:
            _valid = "stopped"
            if workspace.state.lower() != _valid:
                print("-"*40)
                print(f"Cannot perform your request ...")
                print("-"*40 + "\n")
                print(f"Only {_valid.upper()} workspaces can be started.")
                return False
            else:
                _res_start = self.connection.start_workspaces(
                                StartWorkspaceRequests=[ { 'WorkspaceId': workspace.id } ]
                            )
                if len(_res_start["FailedRequests"]) == 0:
                    print("-"*40)
                    print(f"Succesful request to start: {workspace.id}")
                    print("-"*40 + "\n")
                    return True
                else:
                    return False
        except Exception as e:
            print(f"Failed to start workspace: {e}")
            raise e


    def stop_workspace(self, workspace):
        try:
            _valid = ["available", "impaired", "unhealthy", "error"]
            if workspace.state.lower() not in _valid:
                print("-"*40)
                print(f"Cannot perform your request ...")
                print("-"*40 + "\n")
                print(f"{workspace.state.upper()} workspaces cannot be stopped.")
                return False
            else:
                _res_stop = self.connection.stop_workspaces(
                                StopWorkspaceRequests=[ { 'WorkspaceId': workspace.id } ]
                            )
                if len(_res_stop["FailedRequests"]) == 0:
                    print("-"*40)
                    print(f"Succesful request to stop: {workspace.id}")
                    print("-"*40 + "\n")
                    return True
                else:
                    return False
        except Exception as e:
            print(f"Failed to stop workspace: {e}")
            raise e


    def terminate_workspace(self, workspace):
        try:
            _invalid = "suspended"
            if workspace.state.lower() == _invalid:
                print("-"*40)
                print(f"Cannot perform your request ...")
                print("-"*40 + "\n")
                print(f"{workspace.state.upper()} workspaces cannot be terminated.")
                return False
            else:
                _res_terminate = self.connection.terminate_workspaces(
                                TerminateWorkspaceRequests=[ { 'WorkspaceId': workspace.id } ]
                            )
                if len(_res_terminate["FailedRequests"]) == 0:
                    print("-"*40)
                    print(f"Succesful request to terminate: {workspace.id}")
                    print("-"*40 + "\n")
                    return True
                else:
                    return False
        except Exception as e:
            print(f"Failed to terminate workspace: {e}")
            raise e


class Manager:
    """State Manager"""
    def __init__(self):
        self.rds = None 
        self.wks = None
        self.directory = None
        self.workspaces = []
        self.actions = {}
        self.current_action = None 
        self.current_workspace = None


    def bootstrap(self):
        """Get directory and workspaces from AWS"""
        os.system("clear")

        try:
            """Fetching Actions"""
            _rds = RDS()
            _rds.connect()

            if _rds.connection is not None:
                _actions = _rds.get_actions()
                self.actions = _actions

            self.rds = _rds

            _wks = WKS()
            _wks.connect()

            """Fetching Directory"""
            if _wks.connection is not None:
                _directory = _wks.get_directory()
                self.directory = _directory

            """Fetching Workspaces"""
            if self.directory is not None:
                if _wks.connection is not None:
                    _workspaces = _wks.get_workspaces()
                    self.workspaces = _workspaces

            self.wks = _wks

        except Exception as e:
            print(f"Failure bootstrapping Manager: {e}")


    def refresh_workspaces(self):
        """Refresh workspaces"""

        try:
            _wks = self.wks
            _wks.connect()

            """Fetching Workspaces"""
            if _wks.connection is not None:
                _workspaces = _wks.get_workspaces()
                self.workspaces = _workspaces

        except Exception as e:
            print(f"Failure bootstrapping Manager: {e}")


    def show_workspaces(self):
        """Pretty-print the properties of our workspaces"""
        _count = len(self.workspaces)
        print(f"\nHello!, we have {_count} workspace(s): \n")
        if _count > 0:
            print("="*75)
            print("{:^15s}{:^15s}{:^15s}{:^15s}{:^15}".format(
                "ID","UserName","State","Mode","Type"
            ))
            print("="*75)
            for _workspace in self.workspaces:
                print("{:^15s}{:^15s}{:^15s}{:^15s}{:^15}".format(
                    _workspace.id,
                    _workspace.name,
                    _workspace.state,
                    _workspace.mode,
                    _workspace.type,
                ))
            print("="*75)


    def prompt_action(self):
        """Allow user to select an available operation"""
        self.show_workspaces()
        print(f"\nPlease select an action:\n")
        for _index, _label in self.actions.items():
            print(f"[{_index}]: {_label}")
                  
        try: 
            _selected = int(input("\nSelection: "))
            if _selected < 1 or _selected > len(self.actions):
                raise ValueError
            else:
                if self.actions.get(_selected).lower() == "exit":
                    print(f"Good bye!")
                    time.sleep(1)
                    sys.exit(0)
                else:
                    self.current_action = _selected

        except ValueError as ve:
            print("Invalid action!")
            time.sleep(2)
            os.system("clear")
            self.prompt_action()


    def prompt_workspace(self):        
        """Allow user to select an available workspace"""
        os.system("clear")
        self.show_workspaces()
        print(f"\nRequested action: [{self.actions.get(self.current_action)}]")

        print(f"\nPlease select a workspace:\n")
        for _index, _workspace in enumerate(self.workspaces, start=1):
            print(f"[{_index}]: {_workspace.id}")
                  
        try: 
            _selected = int(input("\nSelection: "))
            if _selected < 1 or _selected > len(self.workspaces):
                raise ValueError
            else:
                self.current_workspace = self.workspaces[_selected - 1]

        except ValueError as ve:
            print("Invalid workspace!")
            time.sleep(2)
            self.prompt_workspace()


    def perform_action(self):        
        """Perform action on available workspace"""
        os.system("clear")
        print(f"\nAttempting your requested action ...\n")
        print(f"{self.actions.get(self.current_action)} [{self.current_workspace.id}]\n")

        if self.actions.get(self.current_action).lower() == "start workspace":
            _started = self.wks.start_workspace(self.current_workspace)
        elif self.actions.get(self.current_action).lower() == "stop workspace":
            _stopped = self.wks.stop_workspace(self.current_workspace)
        elif self.actions.get(self.current_action).lower() == "terminate workspace":
            _terminated = self.wks.terminate_workspace(self.current_workspace)                
        else:
            os.system("clear")
            print("\n" + "-"*40)
            print(f"Cannot perform your request ...")
            print("-"*40 + "\n")
            print(f"Operation \"{self.actions.get(self.current_action)}\" has not been implemented.\n")

        time.sleep(5)
        os.system("clear")
        self.refresh_workspaces()
        self.work()


    def work(self):
        if self.directory is None:
            print(f"\nHello!, we have have no directories.")
            print(f"Please create a directory before using this tool.\n")
            sys.exit(1)

        if len(self.workspaces) == 0:
            print(f"\nHello!, we have have no workspaces.")
            print(f"Please create a workspace before using this tool.\n")
            sys.exit(2)

        self.prompt_action()
        self.prompt_workspace()
        self.perform_action()

    
def main():
    """CLI"""
    try:

        _manager = Manager()
        _manager.bootstrap()
        _manager.work()

    except KeyboardInterrupt:
        print(f"Good bye then!")


if __name__ == "__main__":
    main()
    