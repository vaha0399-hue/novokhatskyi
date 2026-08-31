"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";

import {
  ForgotPasswordForm,
  SignInForm,
  SignUpForm,
} from "@/components/auth-forms";
import {
  FAAuthFrame,
  FASuccess,
  type FASuccessKind,
} from "@/components/fa-auth-ui";

type InteractiveView = "signin" | "signup" | "forgot" | "success";

export function FAAuthExperience({
  initialView,
  nextPath = "/matches",
  initialMessage = null,
}: {
  initialView: Exclude<InteractiveView, "success">;
  nextPath?: string;
  initialMessage?: string | null;
}) {
  const router = useRouter();
  const [view, setView] = useState<InteractiveView>(initialView);
  const [successKind, setSuccessKind] = useState<FASuccessKind>("signin");
  const [email, setEmail] = useState("");

  function complete(kind: FASuccessKind) {
    setSuccessKind(kind);
    setView("success");
  }

  function completeSuccessAction() {
    if (successKind === "signin") {
      router.replace(nextPath);
      router.refresh();
      return;
    }
    setView("signin");
  }

  return (
    <FAAuthFrame view={view}>
      {view === "signin" ? (
        <SignInForm
          email={email}
          setEmail={setEmail}
          nextPath={nextPath}
          initialMessage={initialMessage}
          onForgot={() => setView("forgot")}
          onSignup={() => setView("signup")}
          onSuccess={() => complete("signin")}
        />
      ) : null}
      {view === "signup" ? (
        <SignUpForm
          email={email}
          setEmail={setEmail}
          nextPath={nextPath}
          onSignin={() => setView("signin")}
          onSuccess={() => complete("signup")}
        />
      ) : null}
      {view === "forgot" ? (
        <ForgotPasswordForm
          email={email}
          setEmail={setEmail}
          initialMessage={initialMessage}
          onBack={() => setView("signin")}
          onSuccess={() => complete("reset")}
        />
      ) : null}
      {view === "success" ? <FASuccess kind={successKind} onAction={completeSuccessAction} /> : null}
    </FAAuthFrame>
  );
}
