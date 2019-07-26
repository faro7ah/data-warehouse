import boto3
import config
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


def delete_stack():

    """
    Delete the Sparkify stack.
    """

    cloudformation.delete_stack(
        StackName=config.CLOUDFORMATION_STACK_NAME
    )


def delete_sparkify_stack():

    """
    Launches the Sparkify stack deletion and waits for
    the defined resources to be also removed.
    """

    def info(text):
        return print('{} | {}'.format(
            time.strftime('%H:%M:%S.%f', time.gmtime()),
            text
        ))

    info('Deleting the stack...')
    # delete_stack()

    info('Deleting the resources...')
    info('This process may take awhile, please be patient')
    while True:
        status = get_stack_status()
        info('Current status: {}'.format(status))
        if status == 'DELETE_COMPLETE':
            info('Resources deleted :-)')
            break
        info('Asking again in {} seconds'.format(delay))
        time.sleep(delay)


if __name__ == '__main__':
    delete_sparkify_stack()
