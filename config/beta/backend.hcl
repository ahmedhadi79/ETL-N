region="eu-west-2"
bucket="bb2-beta-tfstate"
key="data-lake-etl.tfstate"
encrypt="true"
use_lockfile="true"
assume_role = {
    role_arn="arn:aws:iam::521333308695:role/tfstate-mgnt-role-data-lake-etl-beta"
    session_name="data-lake-etl-beta"
}