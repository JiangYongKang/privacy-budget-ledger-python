def test_skeleton_imports():
    """骨架冒烟：所有模块可导入、账本可实例化。"""
    from privacy_budget_ledger.api import create_app
    from privacy_budget_ledger.ledger import BudgetLedger

    assert BudgetLedger is not None
    assert create_app is not None
