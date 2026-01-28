"""Shared configuration for CDK stacks."""
class StackConfig:
    """ Encapsulates configuration shared across CDK stacks."""
    def __init__(self, org, project, team, target_account, environment, branch_name):
        self.org = org
        self.team = team
        self.project = project
        self.target_account=target_account
        self._environment = environment
        self.branch_name = branch_name
