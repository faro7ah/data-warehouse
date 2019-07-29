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

# A reference to the builtin function 'print()'.
builtin_print = print


def print(text):

    """
    Prints a timestamp next to the the given text.

    Args:
        text (str): The text to print.
    """

    return builtin_print('{} | {}'.format(
        time.strftime('%H:%M:%S', time.gmtime()),
        text
    ))


def get_output_value(description, key):

    """
    Get an output value of a given stack description.

    Args:
        description (dict): The stack description object.
        key (str): The key of the output.

    Returns:
        (str): The value of the output.
    """

    outputs = [o for o in description['Outputs'] if o['OutputKey'] == key]
    return None if len(outputs) != 1 else outputs[0]['OutputValue']


def get_stack_info():

    """
    Gets the description of the Sparkify stack.

    Returns:
        (dict): The description of the stack.
    """

    response = cloudformation.describe_stacks(
        StackName=config.CLOUDFORMATION_STACK_NAME
    )
    return response['Stacks'][0]


def create_stack():

    """
    Creates the Sparkify stack.

    Returns:
        (str): The identifier of the stack.
    """

    with open(os.path.join(os.getcwd(), 'sparkify_stack.json'), 'r') as f:
        content = f.read()

    cloudformation.create_stack(
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


def create_sparkify_stack():

    """
    Launches the Sparkify stack creation and waits for the defined
    resources to be also created and ready to use.
    """

    # Creates the stack.
    print('Creating the stack. This may take awhile, please be patient.')
    create_stack()

    # Until the resources are provisioned.
    while True:

        # Gets the info corresponding the stack.
        description = get_stack_info()
        print('Current status: {}'.format(description['StackStatus']))

        # If the resources are provisioned.
        if description['StackStatus'] == 'CREATE_COMPLETE':

            # Prints the role ARN and the cluster endpoint.
            print('Role ARN: {}'.format(
                get_output_value(description, 'SparkifyRoleArn')
            ))
            print('Cluster endpoint: {}'.format(
                get_output_value(description, 'SparkifyClusterEndpoint')
            ))
            print('Resources created :-)')
            break

        # Waits a few seconds until try again.
        print('Asking again in {} seconds'.format(delay))
        time.sleep(delay)


if __name__ == '__main__':
    create_sparkify_stack()
