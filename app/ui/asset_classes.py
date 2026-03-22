"""Подклассы: редактируемые целевые доли (% портфеля). Доли классов считаются автоматически."""
import streamlit as st

from app.db import list_asset_classes, list_asset_subclasses, update_asset_subclass_target


def render_asset_classes():
    st.caption(
        "Задавайте **только подклассы** (в % всего портфеля). Доля **класса** = сумма подклассов и "
        "обновляется сама при сохранении."
    )
    classes = list_asset_classes()
    subclasses = list_asset_subclasses()
    sub_by_class = {}
    for s in subclasses:
        sub_by_class.setdefault(s.asset_class_id, []).append(s)

    total_all = sum(float(s.target_pct) for s in subclasses)
    if abs(total_all - 100.0) > 0.05:
        st.warning(f"Сумма по всем подклассам: **{total_all:.3f}%** (обычно стремятся к **100%**).")

    for ac in classes:
        subs = sub_by_class.get(ac.id, [])
        sub_sum = sum(float(s.target_pct) for s in subs)
        with st.expander(f"{ac.name} — **{sub_sum:.3f}%** портфеля (сумма подклассов)"):
            st.caption("Доля класса не редактируется вручную.")
            for s in subs:
                cur = round(float(s.target_pct), 3)
                spct = st.number_input(
                    f"  {s.name} (%)",
                    min_value=0.0,
                    max_value=100.0,
                    value=cur,
                    step=0.001,
                    format="%.3f",
                    key=f"sub_{s.id}",
                )
                if round(spct, 3) != cur and st.button(f"Сохранить {s.name}", key=f"btn_sub_{s.id}"):
                    update_asset_subclass_target(s.id, spct)
                    st.success("Сохранено.")
                    st.rerun()
