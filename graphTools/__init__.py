"""
    A Python package containing a collection of level1 rule functions.

IMPORTED FUNCTIONS:
    json
    pathlib (Path)

THIRD PARTY FUNCTIONS:
    workingdays (date_utilities)

FUNCTIONS:
    outputSetup(hliDictObj, stateObj)
    customerDGKey(hliDictObj, stateObj)
    supplierDGKey(hliDictObj, stateObj)
    hliKey(hliDictObj, stateObj)
    sourceRegionRule(hliDictObj, stateObj)
    flowTypeRule(hliDictObj, stateObj)
    bridgeKey(hliDictObj, stateObj)
    intercompanyKey(hliDictObj, stateObj)
    activeHeaderHoldStatusRule(hliDictObj, stateObj)
    inactiveHeaderHoldStatusRule(hliDictObj, stateObj)
    activeItemHoldStatusRule(hliDictObj, stateObj)
    inactiveItemHoldStatusRule(hliDictObj, stateObj)
    hliFEHoldsRule(hliDictObj, stateObj)
    hliSCHoldsRule(hliDictObj, stateObj)
    bloodBuildRule(hliDictObj, stateObj)
    emrRule(hliDictObj, stateObj)
    milestonesCustomerPOCreatedRule(hliDictObj, stateObj)
    milestonesOrderReceivedRule(hliDictObj, stateObj)
    milestonesOrderCreatedRule(hliDictObj, stateObj)
    milestonesItemCreatedRule(hliDictObj, stateObj)
    milestonesItemCleanedRule(hliDictObj, stateObj)
    milestonesReleasedToProductionRule(hliDictObj, stateObj)
    milestonesProductionDoneRule(hliDictObj, stateObj)
    systemESDHliScheduleDateRule(hliDictObj, stateObj)
    hliSoftCommitFactoryShipDate(hliDictObj, stateObj)
    hliHardCommitFactoryShipDate(hliDictObj, stateObj)
    hliSoftCommitCustomerShipDate(hliDictObj, stateObj)
    hliHardCommitCustomerShipDate(hliDictObj, stateObj)
    hliSoftCommitDeliveryDate(hliDictObj, stateObj)
    hliHardCommitDeliveryDate(hliDictObj, stateObj)

MISC VARIABLES:
    __version__
    __all__

DEPENDENCY FILES:
    ruleMap.json
"""
from . _version import version as __version__

__all__ = ['hli_rules', '_version']


def main():
    printVersion = 'version: {}'
    return print(printVersion.format(__version__))


if __name__ == "__main__":
    main()
