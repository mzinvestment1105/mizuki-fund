import os
from pathlib import Path

import jquantsapi


def main() -> None:
    api_key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise ValueError(
            "JQUANTS_API_KEY が未設定です。"
            "ダッシュボードで発行した API Key を環境変数 JQUANTS_API_KEY に設定してください。"
        )

    client = jquantsapi.ClientV2(api_key=api_key)

    # v2: /equities/master (eq-master)
    df = client.get_eq_master()

    # v2 market code: "0111" = Prime (プライム)
    prime = df[df["Mkt"].astype(str) == "0111"].copy()

    out = prime[["Code", "CoName", "MktNm"]].rename(
        columns={"CoName": "CompanyName", "MktNm": "MarketCodeName"}
    )
    out["Code"] = out["Code"].astype(str)
    out = out.sort_values(["Code"]).reset_index(drop=True)

    output_path = Path("data") / "universe" / "prime_list.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"saved: {output_path} ({len(out)} rows)")


if __name__ == "__main__":
    main()
