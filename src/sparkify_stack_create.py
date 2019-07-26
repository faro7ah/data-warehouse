import boto3
import config
import os
import time


# The CloudFormation client.
cloudformation = boto3.client(
    'cloudformation',
    region_name=config.AWS_REGION,
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
)

# The delay between stack status checks.
delay = 30


def get_stack_status():

    """
    Checks the status of the Sparkify stack.

    Returns:
        (str): The status of the stack.
    """

    response = cloudformation.describe_stacks(
        StackName=config.CLOUDFORMATION_STACK_NAME
    )
    return response['Stacks'][0]['StackStatus']


def create_stack():

    """
    Creates the Sparkify stack.

    Returns:
        (str): The identifier of the stack.
    """

    with open(os.path.join(os.getcwd(), 'sparkify-stack.json'), 'r') as f:
        content = f.read()

    response = cloudformation.create_stack(
        StackName=config.CLOUDFORMATION_STACK_NAME,
        TemplateBody=content,
        Capabilities=['CAPABILITY_NAMED_IAM'],
        Parameters=[
            {
                'ParameterKey': 'RoleNameParam',
                'ParameterValue': config.IAM_ROLE_NAME
            },
            {
                'ParameterKey': 'ClusterIdentifierParam',
                'ParameterValue': config.REDSHIFT_CLUSTER_IDENTIFIER
            },
            {
                'ParameterKey': 'ClusterTypeParam',
                'ParameterValue': config.REDSHIFT_CLUSTER_TYPE
            },
            {
                'ParameterKey': 'NodeTypeParam',
                'ParameterValue': config.REDSHIFT_NODE_TYPE
            },
            {
                'ParameterKey': 'NumberOfNodesParam',
                'ParameterValue': config.REDSHIFT_NUMBER_OF_NODES
            },
            {
                'ParameterKey': 'DBNameParam',
                'ParameterValue': config.REDSHIFT_DB_NAME
            },
            {
                'ParameterKey': 'PortParam',
                'ParameterValue': config.REDSHIFT_PORT
            },
            {
                'ParameterKey': 'MasterUsernameParam',
                'ParameterValue': config.REDSHIFT_MASTER_USERNAME
            },
            {
                'ParameterKey': 'MasterUserPasswordParam',
                'ParameterValue': config.REDSHIFT_MASTER_USER_PASSWORD
            }
        ]
    )

    return response['StackId']


def create_sparkify_stack():

    """
    Launches the Sparkify stack creation and waits for the defined
    resources to be also created and ready to use.
    """

    def info(text):
        return print('{} | {}'.format(
            time.strftime('%H:%M:%S.%f', time.gmtime()),
            text
        ))

    info('Creating the stack...')
    stack_id = create_stack()
    info('Stack identifier: {}'.format(stack_id))

    info('Provisioning the resources...')
    info('This process may take awhile, please be patient')
    while True:
        status = get_stack_status()
        info('Current status: {}'.format(status))
        if status == 'CREATE_COMPLETE':
            info('Resources created :-)')
            break
        info('Asking again in {} seconds'.format(delay))
        time.sleep(delay)


if __name__ == '__main__':
    create_sparkify_stack()
