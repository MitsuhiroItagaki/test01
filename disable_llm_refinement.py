#!/usr/bin/env python3
"""
LLM推敲機能を無効化するパッチスクリプト
databricks_sql_profiler_analysis.py の refine_report_with_llm 関数を修正
"""

def apply_disable_llm_patch():
    """メインファイルにLLM無効化パッチを適用"""
    
    print("🔧 LLM推敲機能無効化パッチを適用中...")
    
    try:
        # Read the main file
        with open('databricks_sql_profiler_analysis.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find the refine_report_with_llm function
        function_start = content.find('def refine_report_with_llm(raw_report: str, query_id: str) -> str:')
        
        if function_start == -1:
            print("❌ refine_report_with_llm 関数が見つかりません")
            return False
        
        # Find the function body start
        docstring_end = content.find('"""', function_start)
        docstring_end = content.find('"""', docstring_end + 1) + 3
        
        # Insert the disable check right after the docstring
        disable_check = '''
    
    # 🚫 LLM推敲機能無効化チェック
    print("🚫 LLM推敲機能を無効化しています。クリーンなレポートを返します。")
    return raw_report
'''
        
        # Insert the disable check
        new_content = content[:docstring_end] + disable_check + content[docstring_end:]
        
        # Write back to file
        with open('databricks_sql_profiler_analysis.py', 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print("✅ LLM推敲機能無効化パッチを適用しました")
        print("📄 これで output_optimization_report_XXXX.md ファイルがクリーンになります")
        return True
        
    except Exception as e:
        print(f"❌ パッチ適用中にエラーが発生しました: {e}")
        return False

def revert_llm_patch():
    """LLM無効化パッチを元に戻す"""
    
    print("🔄 LLM推敲機能を有効化に戻しています...")
    
    try:
        # Read the main file
        with open('databricks_sql_profiler_analysis.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Remove the disable check
        disable_check = '''
    
    # 🚫 LLM推敲機能無効化チェック
    print("🚫 LLM推敲機能を無効化しています。クリーンなレポートを返します。")
    return raw_report
'''
        
        new_content = content.replace(disable_check, '')
        
        # Write back to file
        with open('databricks_sql_profiler_analysis.py', 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print("✅ LLM推敲機能を有効化に戻しました")
        return True
        
    except Exception as e:
        print(f"❌ 復元中にエラーが発生しました: {e}")
        return False

if __name__ == "__main__":
    print("🚫 LLM推敲機能無効化ツール")
    print("=" * 50)
    print("1. 無効化パッチを適用")
    print("2. 有効化に戻す")
    print("=" * 50)
    
    choice = input("選択してください (1 or 2): ").strip()
    
    if choice == "1":
        if apply_disable_llm_patch():
            print("\n" + "=" * 50)
            print("✅ 完了！")
            print("📋 次回の実行時から、クリーンなレポートが生成されます")
            print("🚫 LLM推敲による見出し構造の問題が解決されます")
            print("=" * 50)
        else:
            print("❌ パッチの適用に失敗しました")
    
    elif choice == "2":
        if revert_llm_patch():
            print("\n" + "=" * 50)
            print("✅ 完了！")
            print("🤖 LLM推敲機能が再び有効になりました")
            print("=" * 50)
        else:
            print("❌ 復元に失敗しました")
    
    else:
        print("❌ 無効な選択です") 