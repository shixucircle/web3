#for i in $(cat list); do echo python3 store_contract_abi.py ethereum $i; done

for i in $(cat list_eth); do
    echo python3 store_contract_abi.py ethereum $i
    python3 store_contract_abi.py ethereum $i
done
